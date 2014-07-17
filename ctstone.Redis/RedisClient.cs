using ctstone.Redis.Internal;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    public partial class RedisClient : IDisposable
    {
        const int DefaultPort = 6379;
        readonly RedisConnection _connection;
        readonly RedisTransaction _transaction;
        readonly SubscriptionListener _subscription;
        readonly MonitorListener _monitor;
        string _authPassword;

        public string Host { get { return _connection.Host; } }
        public int Port { get { return _connection.Port; } }
        public bool Connected { get { return _connection.Connected; } }
        public Encoding Encoding { get { return _connection.Encoding; } }
        public int ReadTimeout
        {
            get { return _connection.ReadTimeout; }
            set { _connection.ReadTimeout = value; }
        }
        public int SendTimeout
        {
            get { return _connection.SendTimeout; }
            set { _connection.SendTimeout = value; }
        }
        public int ReconnectAttempts
        {
            get { return _connection.ReconnectAttempts; }
            set { _connection.ReconnectAttempts = value; }
        }
        public int ReconnectTimeout
        {
            get { return _connection.ReconnectTimeout; }
            set { _connection.ReconnectTimeout = value; }
        }

        public event Action<RedisSubscriptionMessage> SubscriptionReceived;
        public event Action<RedisSubscriptionChannel> SubscriptionChanged;
        public event Action<object> MonitorReceived;


        public RedisClient(string host)
            : this(host, DefaultPort)
        { }
        public RedisClient(string host, int port)
            : this(host, port, new UTF8Encoding(false))
        { }
        public RedisClient(string host, int port, Encoding encoding)
            : this(new DefaultConnector(host, port), encoding)
        { }
        internal RedisClient(IRedisConnector connector, Encoding encoding)
        {
            _connection = new RedisConnection(connector, encoding);
            _transaction = new RedisTransaction(_connection);
            _subscription = new SubscriptionListener(_connection);
            _monitor = new MonitorListener(_connection);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
            _monitor.MonitorReceived += OnMonitorReceived;
            _connection.Reconnected += OnConnectionReconnected;
        }

        
        
        public void StartPipe()
        {
            _connection.BeginPipe();
        }

        public void StartPipeTransaction()
        {
            _connection.BeginPipe();
            Multi();
        }

        public object[] EndPipe()
        {
            if (_transaction.Active)
                return _transaction.Execute();
            else
                return _connection.EndPipe();
        }
        
        public void Dispose()
        {
            _connection.Dispose();
        }

        void OnMonitorReceived(object obj)
        {
            if (MonitorReceived != null)
                MonitorReceived(obj);
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
            if (_authPassword != null)
                Auth(_authPassword);
        }
    }
}
