using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    class RedisSubscriptionDispatcher
    {
        public event EventHandler<RedisSubscriptionReceivedEventArgs> MessageReceived;

        public void OnMsgReceived(RedisSubscriptionMessage message)
        {
            if (MessageReceived != null)
                MessageReceived(null, new RedisSubscriptionReceivedEventArgs(message));
        }
    }

    /// <summary>
    /// Thread-safe redis subscription client
    /// </summary>
    public class RedisSubscriptionClient : IDisposable
    {
        private readonly RedisConnection _connection;
        private readonly Task _reader;
        private readonly CancellationTokenSource _readCancel;
        private readonly Dictionary<string, RedisSubscriptionDispatcher> _callbackDispatchers;
        private ActivityTracer _activity;
        private long _count;

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Get a value indicating that the current RedisSubscriptionClient is connected to the server
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get the total number of subscribed channels
        /// </summary>
        public long Count { get { return _count; } }

        /// <summary>
        /// Create new instance of subscribe-only RedisClient
        /// </summary>
        /// <param name="host">Redis server host or IP</param>
        /// <param name="port">Redis server port</param>
        /// <param name="password">Redis server password</param>
        public RedisSubscriptionClient(string host, int port, string password = null)
        {
            _activity = new ActivityTracer("New Redis subscription client");
            _connection = new RedisConnection(host, port);
            _connection.Connect(0, 1000);
            if (!String.IsNullOrEmpty(password))
            {
                var cmd = RedisCommand.Auth(password);
                _connection.Call(cmd.Parser, cmd.Command, cmd.Arguments);
            }
            _readCancel = new CancellationTokenSource();
            _reader = Task.Factory.StartNew(Read_Task);
            _callbackDispatchers = new Dictionary<string, RedisSubscriptionDispatcher>();
        }

        /// <summary>
        /// Listen for messages published to the given channels
        /// </summary>
        /// <param name="channels">Channels to subscribe</param>
        public void Subscribe(params string[] channels)
        {
            Subscribe(null, channels);
        }

        /// <summary>
        /// Listen for messages published to the given channels
        /// </summary>
        /// <param name="callback">Callback for received messages on the specified channels</param>
        /// <param name="channels">Channels to subscribe</param>
        public void Subscribe(Action<RedisSubscriptionMessage> callback, params string[] channels)
        {
            if (callback != null)
                AddCallback(callback, channels);
            var cmd = RedisCommand.Subscribe(channels);
            _connection.Write(cmd.Command, cmd.Arguments);
        }

        /// <summary>
        /// Listen for messages published to channels matching the given patterns
        /// </summary>
        /// <param name="channelPatterns">Patterns to subscribe</param>
        public void PSubscribe(params string[] channelPatterns)
        {
            PSubscribe(null, channelPatterns);
        }

        /// <summary>
        /// Listen for messages published to channels matching the given patterns
        /// </summary>
        /// <param name="callback">Callback for received messages on the specified channel patterns</param>
        /// <param name="channelPatterns">Patterns to subscribe</param>
        public void PSubscribe(Action<RedisSubscriptionMessage> callback, params string[] channelPatterns)
        {
            if (callback != null)
                AddCallback(callback, channelPatterns);
            var cmd = RedisCommand.PSubscribe(channelPatterns);
            _connection.Write(cmd.Command, cmd.Arguments);
        }

        /// <summary>
        /// Stop listening for messages posted to the given channels
        /// </summary>
        /// <param name="channels">Channels to unsubscribe</param>
        public void Unsubscribe(params string[] channels)
        {
            RemoveCallback(channels);
            var cmd = RedisCommand.Unsubscribe(channels);
            _connection.Write(cmd.Command, cmd.Arguments);
        }

        /// <summary>
        /// Stop listening for messages posted to channels matching the given patterns
        /// </summary>
        /// <param name="channelPatterns">Patterns to unsubscribe</param>
        public void PUnsubscribe(params string[] channelPatterns)
        {
            RemoveCallback(channelPatterns);
            var cmd = RedisCommand.PUnsubscribe(channelPatterns);
            _connection.Write(cmd.Command, cmd.Arguments);
        }

        /// <summary>
        /// Release resources used by the current RedisSubscriptionClient
        /// </summary>
        public void Dispose()
        {
            if (_readCancel != null)
                _readCancel.Cancel();

            if (_reader != null)
            {
                _reader.Wait();
                _reader.Dispose();
            }
            if (_connection != null)
                _connection.Dispose();

            if (_callbackDispatchers != null)
                _callbackDispatchers.Clear();

            _count = 0;

            if (_activity != null)
                _activity.Dispose();
            _activity = null;
        }

        private void AddCallback(Action<RedisSubscriptionMessage> callback, params string[] channels)
        {
            lock (_callbackDispatchers)
            {
                foreach (var channel in channels)
                {
                    RedisSubscriptionDispatcher disp;
                    if (!_callbackDispatchers.TryGetValue(channel, out disp))
                        _callbackDispatchers[channel] = disp = new RedisSubscriptionDispatcher();
                    disp.MessageReceived += (s, a) => { callback(a.Message); };
                }
            }
        }

        private void RemoveCallback(params string[] channels)
        {
            lock (_callbackDispatchers)
            {
                foreach (var channel in channels)
                {
                    RedisSubscriptionDispatcher disp;
                    if (_callbackDispatchers.TryGetValue(channel, out disp))
                        _callbackDispatchers[channel] = null;
                }
            }
        }

        private void Read_Task()
        {
            RedisSubscriptionResponse response;
            using (new ActivityTracer("Handle subscriptions"))
            {
                while (true)
                {
                    if (_readCancel.IsCancellationRequested)
                        break;

                    response = TryReadResponse();
                    if (response == null)
                        continue;

                    switch (response.Type)
                    {
                        case RedisSubscriptionResponseType.Subscribe:
                        case RedisSubscriptionResponseType.PSubscribe:
                        case RedisSubscriptionResponseType.Unsubscribe:
                        case RedisSubscriptionResponseType.PUnsubscribe:
                            RedisSubscriptionChannel channel = response as RedisSubscriptionChannel;
                            Interlocked.Exchange(ref _count, channel.Count);
                            if (SubscriptionChanged != null)
                                SubscriptionChanged(this, new RedisSubscriptionChangedEventArgs(channel));
                            break;

                        case RedisSubscriptionResponseType.Message:
                        case RedisSubscriptionResponseType.PMessage:
                            RedisSubscriptionMessage message = response as RedisSubscriptionMessage;
                            if (SubscriptionReceived != null)
                                SubscriptionReceived(this, new RedisSubscriptionReceivedEventArgs(message));

                            if (message.Pattern != null && _callbackDispatchers.ContainsKey(message.Pattern) && _callbackDispatchers[message.Pattern] != null)
                                _callbackDispatchers[message.Pattern].OnMsgReceived(message);
                            else if (_callbackDispatchers.ContainsKey(message.Channel) && _callbackDispatchers[message.Channel] != null)
                                _callbackDispatchers[message.Channel].OnMsgReceived(message);
                            break;
                    }
                }
            }
        }

        private RedisSubscriptionResponse TryReadResponse()
        {
            try
            {
                return _connection.Read(RedisSubscription.ParseStream);
            }
            catch (IOException e)
            {
                if (e.InnerException != null && e.InnerException is SocketException)
                    return null; // timed out
                throw e; // something else happened
            }
        }
    }


    /// <summary>
    /// Base class for Redis pub/sub responses
    /// </summary>
    public abstract class RedisSubscriptionResponse
    {
        /// <summary>
        /// The type of response
        /// </summary>
        public RedisSubscriptionResponseType Type { get; private set; }

        /// <summary>
        /// Get the channel to which the message was published, or null if not available
        /// </summary>
        public string Channel { get; protected set; }

        /// <summary>
        /// Get the pattern that matched the published channel, or null if not available
        /// </summary>
        public string Pattern { get; protected set; }

        /// <summary>
        /// Read multi-bulk response from Redis server
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        public static RedisSubscriptionResponse ReadResponse(object[] response)
        {
            RedisSubscriptionResponseType type  = ParseType(response[0] as String);

            RedisSubscriptionResponse obj;
            switch (type)
            {
                case RedisSubscriptionResponseType.Subscribe:
                case RedisSubscriptionResponseType.Unsubscribe:
                case RedisSubscriptionResponseType.PSubscribe:
                case RedisSubscriptionResponseType.PUnsubscribe:
                    obj = new RedisSubscriptionChannel(type, response);
                    break;

                case RedisSubscriptionResponseType.Message:
                case RedisSubscriptionResponseType.PMessage:
                    obj = new RedisSubscriptionMessage(type, response);
                    break;

                default:
                    throw new RedisProtocolException("Unexpected response type: " + type);
            }
            obj.Type = type;
            return obj;
        }

        private static RedisSubscriptionResponseType ParseType(string type)
        {
            return (RedisSubscriptionResponseType)Enum.Parse(typeof(RedisSubscriptionResponseType), type, true);
        }
    }

    /// <summary>
    /// Represents a Redis channel in a pub/sub context
    /// </summary>
    public class RedisSubscriptionChannel : RedisSubscriptionResponse
    {
        /// <summary>
        /// Get the number of subscription channels currently open on the current connection
        /// </summary>
        public long Count { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionChannel class
        /// </summary>
        /// <param name="type">The type of channel response</param>
        /// <param name="response">Redis multi-bulk response</param>
        public RedisSubscriptionChannel(RedisSubscriptionResponseType type, object[] response)
        {
            switch (type)
            {
                case RedisSubscriptionResponseType.Subscribe:
                case RedisSubscriptionResponseType.Unsubscribe:
                    Channel = response[1] as String;
                    break;

                case RedisSubscriptionResponseType.PSubscribe:
                case RedisSubscriptionResponseType.PUnsubscribe:
                    Pattern = response[1] as String;
                    break;
            }
            Count = (long)response[2];
            ActivityTracer.Info("Subscription response: type={0}; channel={1}; pattern={2}; count={3}", Type, Channel, Pattern, Count);
        }
    }

    /// <summary>
    /// Represents a Redis message in a pub/sub context
    /// </summary>
    public class RedisSubscriptionMessage : RedisSubscriptionResponse
    {
        /// <summary>
        /// Get the message that was published
        /// </summary>
        public string Body { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionMessage class
        /// </summary>
        /// <param name="type">The type of message response</param>
        /// <param name="response">Redis multi-bulk response</param>
        public RedisSubscriptionMessage(RedisSubscriptionResponseType type, object[] response)
        {
            switch (type)
            {
                case RedisSubscriptionResponseType.Message:
                    Channel = response[1] as String;
                    Body = response[2] as String;
                    break;

                case RedisSubscriptionResponseType.PMessage:
                    Pattern = response[1] as String;
                    Channel = response[2] as String;
                    Body = response[3] as String;
                    break;
            }

            ActivityTracer.Info("Subscription message: type={0}; channel={1}; pattern={2}; body={3}", Type, Channel, Pattern, Body);
        }
    }
}
