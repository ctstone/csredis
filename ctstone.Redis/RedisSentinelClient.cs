using CSRedis.Internal;
using CSRedis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace CSRedis
{
    /// <summary>
    /// Represents a client connection to a Redis sentinel instance
    /// </summary>
    public partial class RedisSentinelClient : IDisposable
    {
        const int DefaultPort = 26379;
        readonly RedisConnection _connection;
        readonly SubscriptionListener _subscription;

        /// <summary>
        /// Raised when a subscription message is received
        /// </summary>
        public event Action<RedisSubscriptionMessage> SubscriptionReceived;

        /// <summary>
        /// Raised when a subscription channel is added or removed
        /// </summary>
        public event Action<RedisSubscriptionChannel> SubscriptionChanged;

        /// <summary>
        /// Raised when the connection has sucessfully reconnected
        /// </summary>
        public event Action Reconnected;

        /// <summary>
        /// Get the Redis sentinel hostname
        /// </summary>
        public string Host { get { return _connection.Host; } }

        /// <summary>
        /// Get the Redis sentinel port
        /// </summary>
        public int Port { get { return _connection.Port; } }

        /// <summary>
        /// Get a value indicating whether the Redis sentinel client is connected to the server
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get the string encoding used to communicate with the server
        /// </summary>
        public Encoding Encoding { get { return _connection.Encoding; } }

        /// <summary>
        /// Get or set the connection read timeout (milliseconds)
        /// </summary>
        public int ReadTimeout
        {
            get { return _connection.ReadTimeout; }
            set { _connection.ReadTimeout = value; }
        }

        /// <summary>
        /// Get or set the connection send timeout (milliseconds)
        /// </summary>
        public int SendTimeout
        {
            get { return _connection.SendTimeout; }
            set { _connection.SendTimeout = value; }
        }

        /// <summary>
        /// Get or set the number of times to attempt a reconnect after a connection fails
        /// </summary>
        public int ReconnectAttempts
        {
            get { return _connection.ReconnectAttempts; }
            set { _connection.ReconnectAttempts = value; }
        }

        /// <summary>
        /// Get or set the amount of time to wait between reconnect attempts
        /// </summary>
        public int ReconnectTimeout
        {
            get { return _connection.ReconnectTimeout; }
            set { _connection.ReconnectTimeout = value; }
        }

        /// <summary>
        /// Create a new RedisSentinelClient using default port and encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        public RedisSentinelClient(string host)
            : this(host, DefaultPort)
        { }

        /// <summary>
        /// Create a new RedisSentinelClient using default encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        /// <param name="port">Redis sentinel port</param>
        public RedisSentinelClient(string host, int port)
            : this(host, port, new UTF8Encoding(false))
        { }

        /// <summary>
        /// Create a new RedisSentinelClient
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        /// <param name="port">Redis sentinel port</param>
        /// <param name="encoding">String encoding</param>
        public RedisSentinelClient(string host, int port, Encoding encoding)
            : this(new DefaultConnector(host, port), encoding)
        { }

        internal RedisSentinelClient(IRedisConnector connector, Encoding encoding)
        {
            _connection = new RedisConnection(connector, encoding);
            _subscription = new SubscriptionListener(_connection);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
            _connection.Reconnected += OnConnectionReconnected;
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

        void OnConnectionReconnected()
        {
            if (Reconnected != null)
                Reconnected();
        }
    }
}
