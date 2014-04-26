using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using ctstone.Redis.Handlers;
using System;
using System.Collections.Generic;

namespace ctstone.Redis
{
    /// <summary>
    /// Syncronous Redis Sentinel client
    /// </summary>
    public class RedisSentinelClient : IDisposable
    {
        private RedisConnection _connection;
        private RedisSubscriptionHandler _subscriptionHandler;
        private ActivityTracer _activity;

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Get a value indicating that the RedisSentinelClient connection is open
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get host that the current RedisSentinelClient is connected to
        /// </summary>
        public string Host { get { return _connection.Host; } }

        /// <summary>
        /// Get the port that the current RedisSentinelClient is connected to
        /// </summary>
        public int Port { get { return _connection.Port; } }

        /// <summary>
        /// Instantiate a new instance of the RedisSentinelClient class
        /// </summary>
        /// <param name="host">Sentinel server hostname or IP</param>
        /// <param name="port">Sentinel server port</param>
        /// <param name="timeout">Connection timeout in milliseconds (0 for no timeout)</param>
        public RedisSentinelClient(string host, int port, int timeout)
        {
            _activity = new ActivityTracer("New Redis Sentinel client");
            _connection = new RedisConnection(host, port);
            _connection.Connect(timeout);
            _subscriptionHandler = new RedisSubscriptionHandler(_connection);
            _subscriptionHandler.SubscriptionChanged += OnSubscriptionChanged;
            _subscriptionHandler.SubscriptionReceived += OnSubscriptionReceived;
        }

        /// <summary>
        /// Call arbitrary Sentinel command (e.g. for a command not yet implemented in this library)
        /// </summary>
        /// <param name="command">The name of the command</param>
        /// <param name="args">Array of arguments to the command</param>
        /// <returns>Redis unified response</returns>
        public object Call(string command, params string[] args)
        {
            return Write(new RedisObject(command, args));
        }

        /// <summary>
        /// Ping the Sentinel server
        /// </summary>
        /// <returns>Status code</returns>
        public string Ping()
        {
            return Write(RedisCommand.Ping());
        }

        /// <summary>
        /// Get a list of monitored Redis masters
        /// </summary>
        /// <returns>Redis master info</returns>
        public RedisMasterInfo[] Masters()
        {
            return Write(RedisCommand.Masters());
        }

        /// <summary>
        /// Get a list of other Sentinels known to the current Sentinel
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Sentinel hosts and ports</returns>
        public RedisSentinelInfo[] Sentinels(string masterName)
        {
            return Write(RedisCommand.Sentinels(masterName));
        }


        /// <summary>
        /// Get a list of monitored Redis slaves to the given master 
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Redis slave info</returns>
        public RedisSlaveInfo[] Slaves(string masterName)
        {
            return Write(RedisCommand.Slaves(masterName));
        }

        /// <summary>
        /// Get the IP and port of the current master Redis server
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>IP and port of master Redis server</returns>
        public Tuple<string, int> GetMasterAddrByName(string masterName)
        {
            return Write(RedisCommand.GetMasterAddrByName(masterName));
        }

        /// <summary>
        /// Open one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channels">Name of channels to open (refer to http://redis.io/ for channel names)</param>
        public void Subscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Subscribe(channels));
        }

        /// <summary>
        /// Close one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channels">Name of channels to close</param>
        public void Unsubscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Unsubscribe(channels));
        }

        /// <summary>
        /// Open one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channelPatterns">Pattern of channels to open (refer to http://redis.io/ for channel names)</param>
        public void PSubscribe(params string[] channelPatterns)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.PSubscribe(channelPatterns));
        }

        /// <summary>
        /// Close one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channelPatterns">Pattern of channels to close</param>
        public void PUnsubscribe(params string[] channelPatterns)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.PUnsubscribe(channelPatterns));
        }

        /// <summary>
        /// Release resoures used by the current RedisSentinelClient
        /// </summary>
        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
            if (_activity != null)
                _activity.Dispose();
            _activity = null;
        }

        private T Write<T>(RedisCommand<T> command)
        {
            if (!_connection.Connected)
                throw new InvalidOperationException("RedisSentinelClient is not connected");

            return _connection.Call(command.Parser, command.Command, command.Arguments);
        }

        private void OnSubscriptionReceived(object sender, RedisSubscriptionReceivedEventArgs e)
        {
            if (SubscriptionReceived != null)
                SubscriptionReceived(this, e);
        }

        private void OnSubscriptionChanged(object sender, RedisSubscriptionChangedEventArgs e)
        {
            if (SubscriptionChanged != null)
                SubscriptionChanged(this, e);
        }
    }

    /// <summary>
    /// Base class for Redis server-info objects reported by Sentinel
    /// </summary>
    public abstract class RedisServerInfo
    {
        /// <summary>
        /// Get or set Redis server name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Get or set Redis server IP
        /// </summary>
        public string Ip { get; set; }

        /// <summary>
        /// Get or set Redis server port
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Get or set Redis server run ID
        /// </summary>
        public string RunId { get; set; }

        /// <summary>
        /// Get or set Redis server flags
        /// </summary>
        public string Flags { get; set; }

        /// <summary>
        /// Get or set number of pending Redis server commands
        /// </summary>
        public long PendingCommands { get; set; }

        /// <summary>
        /// Get or set milliseconds since last successful ping reply
        /// </summary>
        public long LastOkPingReply { get; set; }

        /// <summary>
        /// Get or set milliseconds since last ping reply
        /// </summary>
        public long LastPingReply { get; set; }
    }

    /// <summary>
    /// Represents a Redis master node as reported by a Redis Sentinel
    /// </summary>
    public class RedisMasterInfo : RedisServerInfo
    {
        /// <summary>
        /// Get or set a timestamp(?)
        /// </summary>
        public long InfoRefresh { get; set; }

        /// <summary>
        /// Get or set number of slaves of the current master node
        /// </summary>
        public int NumSlaves { get; set; }

        /// <summary>
        /// Get or set number of other Sentinels
        /// </summary>
        public int NumOtherSentinels { get; set; }

        /// <summary>
        /// Get or set Sentinel quorum count
        /// </summary>
        public int Quorum { get; set; }
    }

    /// <summary>
    /// Represents a Redis Sentinel node as reported by a Redis Sentinel
    /// </summary>
    public class RedisSentinelInfo : RedisServerInfo
    {
        /// <summary>
        /// Get or set milliseconds(?) since last hello message from current Sentinel node
        /// </summary>
        public long LastHelloMessage { get; set; }

        /// <summary>
        /// Get or set value indicating that current Sentinel is allowed to fail-over the master
        /// </summary>
        public bool CanFailoverItsMaster { get; set; }
    }

    /// <summary>
    /// Represents a Redis slave node as reported by a Redis Setinel
    /// </summary>
    public class RedisSlaveInfo : RedisServerInfo
    {
        /// <summary>
        /// Get or set a timestamp(?)
        /// </summary>
        public long InfoRefresh { get; set; }

        /// <summary>
        /// Get or set milliseconds(?) that master link has been down
        /// </summary>
        public long MasterLinkDownTime { get; set; }

        /// <summary>
        /// Get or set status of master link
        /// </summary>
        public string MasterLinkStatus { get; set; }

        /// <summary>
        /// Get or set the master host of the current Redis slave node
        /// </summary>
        public string MasterHost { get; set; }

        /// <summary>
        /// Get or set the master port of the current Redis slave node
        /// </summary>
        public int MasterPort { get; set; }

        /// <summary>
        /// Get or set the priority of the current Redis slave node
        /// </summary>
        public int SlavePriority { get; set; }
    }
}
