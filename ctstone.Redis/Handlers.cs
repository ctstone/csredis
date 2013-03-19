using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ctstone.Redis
{
    class RedisSubscriptionHandler
    {
        private readonly RedisConnection _connection;

        public bool IsSubscribed { get; private set; }
        public long Count { get; private set; }

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        public RedisSubscriptionHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public void HandleSubscription(RedisSubscription command)
        {
            _connection.Write(command.Command, command.Arguments);
            if (!IsSubscribed)
            {
                IsSubscribed = true;
                while (true)
                {
                    var resp = _connection.Read(command.Parser);
                    switch (resp.Type)
                    {
                        case RedisSubscriptionResponseType.Subscribe:
                        case RedisSubscriptionResponseType.PSubscribe:
                        case RedisSubscriptionResponseType.Unsubscribe:
                        case RedisSubscriptionResponseType.PUnsubscribe:
                            RedisSubscriptionChannel channel = resp as RedisSubscriptionChannel;
                            Count = channel.Count;
                            if (SubscriptionChanged != null)
                                SubscriptionChanged(this, new RedisSubscriptionChangedEventArgs(channel));
                            break;

                        case RedisSubscriptionResponseType.Message:
                        case RedisSubscriptionResponseType.PMessage:
                            RedisSubscriptionMessage message = resp as RedisSubscriptionMessage;
                            if (SubscriptionReceived != null)
                                SubscriptionReceived(this, new RedisSubscriptionReceivedEventArgs(message));
                            break;
                    }
                    if (Count == 0)
                        break;
                }
                IsSubscribed = false;
            }
        }
    }

    class RedisPipelineHandler
    {
        private readonly RedisConnection _connection;
        private long _pipelineCounter;

        public bool Active { get; private set; }

        public RedisPipelineHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public void Start()
        {
            if (Active)
                throw new InvalidOperationException("Already in pipeline mode");
            Active = true;
        }

        public object[] End()
        {
            return End(false);
        }

        public object[] End(bool ignoreResults)
        {
            if (!Active)
                throw new InvalidOperationException("Not in pipeline mode");

            object[] results = null;

            if (!ignoreResults)
                results = new object[_pipelineCounter];

            for (int i = 0; i < _pipelineCounter; i++)
            {
                object result = _connection.Read();
                if (!ignoreResults)
                    results[i] = result;
            }

            Active = false;
            _pipelineCounter = 0;
            return results;
        }

        public void Write(string command, params object[] arguments)
        {
            _pipelineCounter++;
            _connection.WriteAsync(command, arguments);
        }
    }

    class RedisMonitorHandler
    {
        private readonly RedisConnection _connection;

        /// <summary>
        /// Occurs when a monitor response is received
        /// </summary>
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        public RedisMonitorHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public string Monitor()
        {
            string status = _connection.Call(RedisReader.ReadStatus, "MONITOR");
            while (true)
            {
                object message;
                try
                {
                    message = _connection.Read();
                }
                catch (Exception e)
                {
                    if (_connection.Connected) throw e;
                    return status;
                }
                if (MonitorReceived != null)
                    MonitorReceived(this, new RedisMonitorEventArgs(message));
            }
        }
    }
}
