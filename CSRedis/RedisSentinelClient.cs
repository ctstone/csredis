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
        const int DefaultConcurrency = 1000;
        const int DefaultBufferSize = 1024;
        readonly IRedisConnector _connector;
        readonly SubscriptionListener _subscription;

        /// <summary>
        /// Occurs when a subscription message is received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subscription channel is added or removed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Occurs when the connection has sucessfully reconnected
        /// </summary>
        public event EventHandler Reconnected;

        /// <summary>
        /// Get the Redis sentinel hostname
        /// </summary>
        public string Host { get { return _connector.Host; } }

        /// <summary>
        /// Get the Redis sentinel port
        /// </summary>
        public int Port { get { return _connector.Port; } }

        /// <summary>
        /// Get a value indicating whether the Redis sentinel client is connected to the server
        /// </summary>
        public bool Connected { get { return _connector.IsConnected; } }

        /// <summary>
        /// Get the string encoding used to communicate with the server
        /// </summary>
        public Encoding Encoding { get { return _connector.Encoding; } }

        /// <summary>
        /// Get or set the connection read timeout (milliseconds)
        /// </summary>
        public int ReceiveTimeout
        {
            get { return _connector.ReceiveTimeout; }
            set { _connector.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Get or set the connection send timeout (milliseconds)
        /// </summary>
        public int SendTimeout
        {
            get { return _connector.SendTimeout; }
            set { _connector.SendTimeout = value; }
        }

        /// <summary>
        /// Get or set the number of times to attempt a reconnect after a connection fails
        /// </summary>
        public int ReconnectAttempts
        {
            get { return _connector.ReconnectAttempts; }
            set { _connector.ReconnectAttempts = value; }
        }

        /// <summary>
        /// Get or set the amount of time to wait between reconnect attempts
        /// </summary>
        public int ReconnectWait
        {
            get { return _connector.ReconnectWait; }
            set { _connector.ReconnectWait = value; }
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
            : this(host, port, false)
        { }

        /// <summary>
        /// Create a new RedisSentinelClient using default encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        /// <param name="port">Redis sentinel port</param>
        /// <param name="ssl">Set true if Redis sentinel expects SSL</param>
        public RedisSentinelClient(string host, int port, bool ssl)
            : this(new RedisConnector(host, port, ssl, DefaultConcurrency, DefaultBufferSize))
        { }

        internal RedisSentinelClient(IRedisConnector connector)
        {
            _connector = connector;
            _subscription = new SubscriptionListener(_connector);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
            _connector.Connected += OnConnectionReconnected;
        }

        /// <summary>
        /// Release resoures used by the current RedisSentinelClient
        /// </summary>
        public void Dispose()
        {
            if (_connector != null)
                _connector.Dispose();
        }

        void OnSubscriptionReceived(object sender, RedisSubscriptionReceivedEventArgs args)
        {
            if (SubscriptionReceived != null)
                SubscriptionReceived(this, args);
        }

        void OnSubscriptionChanged(object sender, RedisSubscriptionChangedEventArgs args)
        {
            if (SubscriptionChanged != null)
                SubscriptionChanged(this, args);
        }

        void OnConnectionReconnected(object sender, EventArgs args)
        {
            if (Reconnected != null)
                Reconnected(this, args);
        }
    }
}
