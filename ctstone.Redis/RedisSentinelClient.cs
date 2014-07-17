using ctstone.Redis.Internal;
using ctstone.Redis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace ctstone.Redis
{
    /// <summary>
    /// Syncronous Redis Sentinel client
    /// </summary>
    public partial class RedisSentinelClient : IDisposable
    {
        const int DefaultPort = 26379;
        readonly RedisConnection _connection;
        readonly SubscriptionListener _subscription;

        public event Action<RedisSubscriptionMessage> SubscriptionReceived;
        public event Action<RedisSubscriptionChannel> SubscriptionChanged;

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

        public RedisSentinelClient(string host)
            : this(host, DefaultPort)
        { }
        public RedisSentinelClient(string host, int port)
            : this(host, port, new UTF8Encoding(false))
        { }
        public RedisSentinelClient(string host, int port, Encoding encoding)
            : this(new DefaultConnector(host, port), encoding)
        { }
        internal RedisSentinelClient(IRedisConnector connector, Encoding encoding)
        {
            _connection = new RedisConnection(connector, encoding);
            _subscription = new SubscriptionListener(_connection);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
        }

        /// <summary>
        /// Release resoures used by the current RedisSentinelClient
        /// </summary>
        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }

        void OnSubscriptionReceived(RedisSubscriptionMessage message)
        {
            if (SubscriptionReceived != null)
                SubscriptionReceived(message);
        }

        void OnSubscriptionChanged(RedisSubscriptionChannel obj)
        {
            if (SubscriptionChanged != null)
                SubscriptionChanged(obj);
        }
    }
}
