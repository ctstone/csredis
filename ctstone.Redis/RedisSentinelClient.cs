using ctstone.Redis.RedisCommands;
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
        /// Instantiate a new instance of the RedisSentinelClient class
        /// </summary>
        /// <param name="host">Sentinel server hostname or IP</param>
        /// <param name="port">Sentinel server port</param>
        /// <param name="timeout">Connection timeout in milliseconds (0 for no timeout)</param>
        public RedisSentinelClient(string host, int port, int timeout)
        {
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

        public void Subscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Subscribe(channels));
        }

        public void Unsubscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Unsubscribe(channels));
        }

        public void PSubscribe(params string[] channelPatterns)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.PSubscribe(channelPatterns));
        }

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
        public string Name { get; set; }
        public string Ip { get; set; }
        public int Port { get; set; }
        public string RunId { get; set; }
        public string Flags { get; set; }
        public long PendingCommands { get; set; }
        public long LastOkPingReply { get; set; }
        public long LastPingReply { get; set; }
    }

    /// <summary>
    /// Represents a Redis master node as reported by a Redis Sentinel
    /// </summary>
    public class RedisMasterInfo : RedisServerInfo
    {
        public long InfoRefresh { get; set; }
        public int NumSlaves { get; set; }
        public int NumOtherSentinels { get; set; }
        public int Quorum { get; set; }
    }

    /// <summary>
    /// Represents a Redis sentinel node as reported by a Redis Sentinel
    /// </summary>
    public class RedisSentinelInfo : RedisServerInfo
    {
        public long LastHelloMessage { get; set; }
        public bool CanFailoverItsMaster { get; set; }
    }

    /// <summary>
    /// Represents a Redis slave node as reported by a Redis Setinel
    /// </summary>
    public class RedisSlaveInfo : RedisServerInfo
    {
        public long InfoRefresh { get; set; }
        public long MasterLinkDownTime { get; set; }
        public string MasterLinkStatus { get; set; }
        public string MasterHost { get; set; }
        public int MasterPort { get; set; }
        public int SlavePriority { get; set; }
    }
}
