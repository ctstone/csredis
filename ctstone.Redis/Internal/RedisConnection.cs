using CSRedis.Internal.IO;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    class RedisConnection
    {
        readonly Encoding _encoding;
        readonly IRedisConnector _connector;
        readonly ConcurrentQueue<Action> _readQueue;
        readonly ConcurrentQueue<Action> _writeQueue;
        readonly object _readLock = new object();
        readonly object _connectionLock = new object();

        Stream _stream;
        RedisWriter _writer;
        RedisReader _reader;
        RedisPipeline _pipeline;

        /// <summary>
        /// End-of-line string used by Redis server
        /// </summary>
        internal const string EOL = "\r\n";

        public event Action Reconnected;

        public Encoding Encoding { get { return _encoding; } }
        public string Host { get { return _connector.Host; } }
        public int Port { get { return _connector.Port; } }
        public bool Connected { get { return _connector.Connected; } }
        public bool Pipelined { get { return _pipeline.Active; } }
        public int ReadTimeout
        {
            get { return _connector.ReceiveTimeout; }
            set { _connector.ReceiveTimeout = value; }
        }
        public int SendTimeout
        {
            get { return _connector.SendTimeout; }
            set { _connector.SendTimeout = value; }
        }
        public int ReconnectAttempts { get; set; }
        public int ReconnectTimeout { get; set; }

        public RedisConnection(IRedisConnector connector, Encoding encoding)
        {
            ReconnectTimeout = 1500;
            _connector = connector;
            _encoding = encoding;
            _readQueue = new ConcurrentQueue<Action>();
            _writeQueue = new ConcurrentQueue<Action>();
        }

        public bool Connect(int timeout = 0)
        {
            if (timeout > 0)
                ReconnectTimeout = timeout;
            return InitIO(_connector.Connect(timeout));
        }

        public Task<bool> ConnectAsync()
        {
            return _connector.ConnectAsync()
                .ContinueWith(t => InitIO(t.Result));
        }

        public T Call<T>(RedisCommand<T> command)
        {
            try
            {
                if (!Connected)
                    Connect(0);

                lock (_readLock)
                {
                    if (_pipeline.Active)
                    {
                        _pipeline.Write(command);
                        return default(T);
                    }

                    _writer.Write(command.Command, command.Arguments);
                    _connector.OnWriteFlushed();
                    return command.Parse(_reader);
                }
            }
            catch (IOException e)
            {
                if (ReconnectAttempts == 0)
                    throw e;
                Reconnect();
                OnReconnected();
                return Call(command);
            }
        }

        public Task<T> CallAsync<T>(RedisCommand<T> command) // TODO: reconnect
        {
            var tcs = new TaskCompletionSource<T>();
            _writeQueue.Enqueue(() => _writer.WriteAsync(command.Command, command.Arguments));
            _readQueue.Enqueue(() => tcs.SetResult(command.Parse(_reader)));

            ConnectAsync()
                .ContinueWith(x => WriteNext())
                .ContinueWith(x => ReadNext());

            return tcs.Task;
        }

        public void Write<T>(RedisCommand<T> command) // TODO : reconnect
        {
            if (!Connected)
                Connect(0);
            _writer.Write(command.Command, command.Arguments);
            _connector.OnWriteFlushed();
        }

        public Task WriteAsync<T>(RedisCommand<T> command) // TODO: reconnect
        {
            _writeQueue.Enqueue(() => _writer.WriteAsync(command.Command, command.Arguments));
            return ConnectAsync()
                .ContinueWith(x => WriteNext());

        }

        public T Read<T>(Func<RedisReader, T> parser) // TODO: reconnect
        {
            return parser(_reader);
        }

        public Task<T> ReadAsync<T>(Func<RedisReader, T> parser) // TODO: reconnect
        {
            var tcs = new TaskCompletionSource<T>();
            _readQueue.Enqueue(() => tcs.SetResult(parser(_reader)));
            ConnectAsync()
                .ContinueWith(x => ReadNext());
            return tcs.Task;
        }
        
        public void BeginPipe()
        {
            if (!Connected)
                Connect(0);
            _pipeline.Active = true;
        }

        public object[] EndPipe()
        {
            _pipeline.Active = false;
            return _pipeline.Flush();
        }


        public void Dispose()
        {
            _writer.Dispose();
            _connector.Dispose();
            _stream.Dispose();
        }

        void Reconnect()
        {
            _stream.Dispose();
            _stream = null;
            int attempts = 0;
            while (attempts < ReconnectAttempts || ReconnectAttempts == -1)
            {
                Debug.WriteLine(String.Format("Reconnect attempt #{0}/{1}", attempts + 1, ReconnectAttempts));

                if (InitIO(_connector.Reconnect(ReconnectTimeout)))
                    return;

                Thread.Sleep(TimeSpan.FromMilliseconds(ReconnectTimeout));
                attempts++;
            }
            if (!Connected)
                throw new IOException("Could not reconnect after " + attempts + " attempts");
        }

        void OnReconnected()
        {
            if (Reconnected != null)
                Reconnected();
        }

        void WriteNext()
        {
            lock (_readLock)
            {
                Action writer;
                if (_writeQueue.TryDequeue(out writer))
                {
                    writer();
                    _connector.OnWriteFlushed();
                }
            }
        }

        void ReadNext()
        {
            lock (_readLock)
            {
                Action reader;
                if (_readQueue.TryDequeue(out reader))
                    reader();
            }
        }

        bool InitIO(Stream stream)
        {
            lock (_connectionLock)
            {
                if (_stream != null)
                    return true;
            }

            if (stream != null)
            {
                _stream = new BufferedStream(stream);
                _writer = new RedisWriter(_stream, _encoding);
                _reader = new RedisReader(_stream, _encoding);
                _pipeline = new RedisPipeline(_stream, _encoding, _reader);
            }
            return Connected;
        }
    }
}
