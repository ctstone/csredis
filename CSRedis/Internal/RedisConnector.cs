using CSRedis.Internal.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSRedis.Internal
{

    class RedisAsyncThing // TODO: put all the async stuff here
    {
    }


    class RedisConnector : IRedisConnector
    {
        readonly DnsEndPoint _endpoint;
        readonly SocketAsyncEventArgs _connectArgs; // TODO: lazy
        readonly SocketAsyncPool _transferPool; // TODO: lazy
        readonly Task<bool> _connectionTask; // TODO: lazy
        readonly ConcurrentQueue<IRedisAsyncCommandToken> _readQueue; // TODO: lazy
        readonly ConcurrentQueue<IRedisAsyncCommandToken> _writeQueue; // TODO: lazy
        readonly object _readLock; 
        readonly object _writeLock;
        readonly RedisWriter _writer;
        readonly RedisEncoding _encoding;
        Socket _socket;
        BufferedStream _stream;
        RedisReader _reader;
        RedisPipeline _pipeline;
        bool _asyncConnectionStarted;

        public event EventHandler Reconnected;

        public bool Connected { get { return _socket == null ? false : _socket.Connected; } }
        public string Host { get { return _endpoint.Host; } }
        public int Port { get { return _endpoint.Port; } }
        public bool Pipelined { get { return _pipeline.Active; } }
        public int ReconnectAttempts { get; set; }
        public int ReconnectWait { get; set; }
        public int ReceiveTimeout 
        { 
            get { return _socket.ReceiveTimeout; }
            set { _socket.ReceiveTimeout = value; }
        }
        public int SendTimeout 
        { 
            get { return _socket.SendTimeout; }
            set { _socket.SendTimeout = value; }
        }
        public Encoding Encoding
        {
            get { return _encoding.Encoding; }
            set { _encoding.Encoding = value; }
        }
        

        public RedisConnector(string host, int port, int concurrency, int bufferSize)
        {
            _endpoint = new DnsEndPoint(host, port);
            _encoding = new RedisEncoding();
            _transferPool = new SocketAsyncPool(concurrency, bufferSize);
            _readQueue = new ConcurrentQueue<IRedisAsyncCommandToken>();
            _writeQueue = new ConcurrentQueue<IRedisAsyncCommandToken>();
            _readLock = new object();
            _writeLock = new object();
            _writer = new RedisWriter(_encoding);

            var connectionTaskSource = new TaskCompletionSource<bool>();
            _connectArgs = new SocketAsyncEventArgs { RemoteEndPoint = _endpoint, UserToken = connectionTaskSource };
            _connectArgs.Completed += OnSocketCompleted;
            _transferPool.Completed += OnSocketCompleted;
            _connectionTask = connectionTaskSource.Task;
        }

        public bool Connect()
        {
            InitSocket();
            _socket.Connect(_endpoint);

            if (_socket.Connected)
                InitReader();

            return _socket.Connected;
        }

        public Task<bool> ConnectAsync()
        {
            if (!_asyncConnectionStarted)
            {
                lock (_connectArgs)
                {
                    if (!_asyncConnectionStarted)
                    {
                        _asyncConnectionStarted = true;
                        InitSocket();
                        if (!_socket.ConnectAsync(_connectArgs))
                            OnSocketConnected(_connectArgs);
                    }
                }
            }

            return _connectionTask;
        }

        public T Call<T>(RedisCommand<T> command)
        {
            ConnectIfNotConnected();

            try
            {
                if (Pipelined)
                    return _pipeline.Write(command);

                _writer.Write(command, _stream);
                return command.Parse(_reader);
            }
            catch (IOException)
            {
                if (ReconnectAttempts == 0)
                    throw;
                Reconnect();
                return Call(command);
            }
        }

        public Task<T> CallAsync<T>(RedisCommand<T> command)
        {
            var token = new RedisAsyncCommandToken<T>(command);
            _writeQueue.Enqueue(token);
            ConnectAsync().ContinueWith(CallAsyncDeferred);
            return token.TaskSource.Task;
        }

        public void Write(RedisCommand command) // TODO: reconnect
        {
            _writer.Write(command, _stream);
        }

        public T Read<T>(Func<RedisReader, T> func) // TODO: reconnect
        {
            return func(_reader);
        }

        public void Read(Stream destination, int bufferSize) // TODO: reconnect
        {
            _reader.ExpectType(RedisMessage.Bulk);
            _reader.ReadBulk(destination, bufferSize, false);
        }

        public void BeginPipe()
        {
            ConnectIfNotConnected();
            _pipeline.Begin();
        }

        public object[] EndPipe()
        {
            return _pipeline.Flush();
        }

        void CallAsyncDeferred(Task t)
        {
            lock (_writeLock)
            {
                IRedisAsyncCommandToken token;
                if (!_writeQueue.TryDequeue(out token))
                    throw new Exception();

                _readQueue.Enqueue(token);

                var args = _transferPool.Acquire();
                int bytes = _writer.Write(token.Command, args.Buffer, args.Offset); // TODO: check bounds
                args.SetBuffer(args.Offset, bytes);

                if (!_socket.SendAsync(args)) // TODO: catch here
                    OnSocketSent(args);
            }
        }

        void ConnectIfNotConnected()
        {
            if (!Connected)
                Connect();
        }

        void Reconnect()
        {
            int attempts = 0;
            while (attempts++ < ReconnectAttempts || ReconnectAttempts == -1)
            {
                if (Connect())
                {
                    OnReconnected();
                    return;
                }

                Thread.Sleep(TimeSpan.FromMilliseconds(ReconnectWait));
                attempts++;
            }

            throw new IOException("Could not reconnect after " + attempts + " attempts");
        }

        void OnSocketCompleted(object sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Connect:
                    OnSocketConnected(e);
                    break;
                case SocketAsyncOperation.Send:
                    OnSocketSent(e);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        void OnSocketConnected(SocketAsyncEventArgs args)
        {
            if (_socket.Connected)
                InitReader();

            var token = (TaskCompletionSource<bool>)args.UserToken; 
            token.SetResult(_socket.Connected);
        }

        void OnSocketSent(SocketAsyncEventArgs args)
        {
            _transferPool.Release(args);

            IRedisAsyncCommandToken token;
            lock (_readLock)
            {
                if (_readQueue.TryDequeue(out token))
                    token.SetResult(_reader);
            }
        }

        void OnReconnected()
        {
            if (Reconnected != null)
                Reconnected(this, new EventArgs());
        }

        void InitSocket()
        {
            if (_socket != null)
                _socket.Dispose();

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }

        void InitReader()
        {
            if (_stream != null)
                _stream.Dispose();

            _stream = new BufferedStream(new NetworkStream(_socket));
            _reader = new RedisReader (_encoding, _stream);
            _pipeline = new RedisPipeline(_stream, _encoding, _reader);
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
