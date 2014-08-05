using System.Globalization;
using System.Threading;
using CSRedis.Internal;
using System;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CSRedis
{
    /// <summary>
    /// Represents a client connection to a Redis server instance
    /// </summary>
    public partial class RedisClient : IDisposable
    {
        const int DefaultPort = 6379;
        const int DefaultBuffer = 1024;
        readonly RedisConnection _connection;
        readonly RedisTransaction _transaction;
        readonly SubscriptionListener _subscription;
        readonly MonitorListener _monitor;
        bool _streaming;

        /// <summary>
        /// Occurs when a subscription message is received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subscription channel is added or removed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Occurs when a transaction command is acknowledged by the server
        /// </summary>
        public event EventHandler<RedisTransactionQueuedEventArgs> TransactionQueued;

        /// <summary>
        /// Occurs when a monitor message is received
        /// </summary>
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        /// <summary>
        /// Occurs when the connection has sucessfully reconnected
        /// </summary>
        public event EventHandler Reconnected;


        /// <summary>
        /// Get the Redis server hostname
        /// </summary>
        public string Host { get { return _connection.Host; } }

        /// <summary>
        /// Get the Redis server port
        /// </summary>
        public int Port { get { return _connection.Port; } }

        /// <summary>
        /// Get a value indicating whether the Redis client is connected to the server
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get or set the string encoding used to communicate with the server
        /// </summary>
        public Encoding Encoding 
        { 
            get { return _connection.Encoding; }
            set { _connection.Encoding = value; }
        }

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
        /// Create a new RedisClient using default port and encoding
        /// </summary>
        /// <param name="host">Redis server hostname</param>
        public RedisClient(string host)
            : this(host, DefaultPort)
        { }

        /// <summary>
        /// Create a new RedisClient using default encoding
        /// </summary>
        /// <param name="host">Redis server hostname</param>
        /// <param name="port">Redis server port</param>
        public RedisClient(string host, int port)
            : this(host, port, new UTF8Encoding(false))
        { }

        /// <summary>
        /// Create a new RedisClient
        /// </summary>
        /// <param name="host">Redis server hostname</param>
        /// <param name="port">Redis server port</param>
        /// <param name="encoding">String encoding</param>
        public RedisClient(string host, int port, Encoding encoding)
            : this(new DefaultConnector(host, port), encoding)
        { }

        internal RedisClient(IRedisConnector connector, Encoding encoding)
        {
            // use invariant culture - we have to set it explicitly for every thread we create to 
            // prevent any floating-point problems (mostly because of number formats in non en-US cultures).
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;

            _connection = new RedisConnection(connector, encoding);
            _transaction = new RedisTransaction(_connection);
            _subscription = new SubscriptionListener(_connection);
            _monitor = new MonitorListener(_connection);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
            _monitor.MonitorReceived += OnMonitorReceived;
            _connection.Reconnected += OnConnectionReconnected;
            _transaction.TransactionQueued += OnTransactionQueued;
        }

        /// <summary>
        /// Begin buffered pipeline mode (calls return immediately; use EndPipe() to execute batch)
        /// </summary>
        public void StartPipe()
        {
            _connection.BeginPipe();
        }

        /// <summary>
        /// Begin buffered pipeline mode within the context of a transaction (calls return immediately; use EndPipe() to excute batch)
        /// </summary>
        public void StartPipeTransaction()
        {
            _connection.BeginPipe();
            Multi();
        }

        /// <summary>
        /// Execute pipeline commands
        /// </summary>
        /// <returns>Array of batched command results</returns>
        public object[] EndPipe()
        {
            if (_transaction.Active)
                return _transaction.Execute();
            else
                return _connection.EndPipe();
        }
        
        /// <summary>
        /// Stream a BULK reply from the server using default buffer size
        /// </summary>
        /// <typeparam name="T">Response type</typeparam>
        /// <param name="destination">Destination stream</param>
        /// <param name="func">Client command to execute (BULK reply only)</param>
        public void StreamTo<T>(Stream destination, Func<RedisClient, T> func)
        {
            StreamTo(destination, DefaultBuffer, func);
        }

        /// <summary>
        /// Stream a BULK reply from the server
        /// </summary>
        /// <typeparam name="T">Response type</typeparam>
        /// <param name="destination">Destination stream</param>
        /// <param name="bufferSize">Size of buffer used to write server response</param>
        /// <param name="func">Client command to execute (BULK reply only)</param>
        public void StreamTo<T>(Stream destination, int bufferSize, Func<RedisClient, T> func)
        {
            _streaming = true;
            func(this);
            _streaming = false;
            _connection.Read(destination, bufferSize);
        }

        /// <summary>
        /// Dispose all resources used by the current RedisClient
        /// </summary>
        public void Dispose()
        {
            _connection.Dispose();
        }

        void OnMonitorReceived(object sender, RedisMonitorEventArgs obj)
        {
            if (MonitorReceived != null)
                MonitorReceived(this, obj);
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

        void OnTransactionQueued(object sender, RedisTransactionQueuedEventArgs args)
        {
            if (TransactionQueued != null)
                TransactionQueued(this, args);
        }
    }
}
