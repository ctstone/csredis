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
    class RedisConnector : IRedisConnector
    {
        readonly DnsEndPoint _endpoint;
        readonly Lazy<SocketAsyncEventArgs> _asyncConnectArgs;
        readonly Lazy<SocketAsyncPool> _asyncTransferPool;
        readonly Lazy<ConcurrentQueue<IRedisAsyncCommandToken>> _asyncReadQueue;
        readonly Lazy<ConcurrentQueue<IRedisAsyncCommandToken>> _asyncWriteQueue;
        readonly object _readLock; 
        readonly object _writeLock;
        readonly RedisWriter _writer;
        readonly RedisEncoding _encoding;
        readonly int _concurrency;
        readonly int _bufferSize;
        Socket _socket;
        BufferedStream _stream;
        RedisReader _reader;
        RedisPipeline _pipeline;
        bool _asyncConnectionStarted;
        TaskCompletionSource<bool> _connectionTaskSource;

        public event EventHandler Connected;

        public bool IsConnected { get { return _socket == null ? false : _socket.Connected; } }
        public string Host { get { return _endpoint.Host; } }
        public int Port { get { return _endpoint.Port; } }
        public bool IsPipelined { get { return _pipeline.Active; } }
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
        SocketAsyncPool AsyncTransferPool { get { return _asyncTransferPool.Value; } }
        SocketAsyncEventArgs AsyncConnectArgs { get { return _asyncConnectArgs.Value; } }
        ConcurrentQueue<IRedisAsyncCommandToken> AsyncReadQueue { get { return _asyncReadQueue.Value; } }
        ConcurrentQueue<IRedisAsyncCommandToken> AsyncWriteQueue { get { return _asyncWriteQueue.Value; } }

        

        public RedisConnector(string host, int port, int concurrency, int bufferSize)
        {
            _concurrency = concurrency;
            _bufferSize = bufferSize;
            _endpoint = new DnsEndPoint(host, port);
            _encoding = new RedisEncoding();
            _asyncTransferPool = new Lazy<SocketAsyncPool>(SocketAsyncPoolFactory);
            _asyncReadQueue = new Lazy<ConcurrentQueue<IRedisAsyncCommandToken>>(AsyncQueueFactory);
            _asyncWriteQueue = new Lazy<ConcurrentQueue<IRedisAsyncCommandToken>>(AsyncQueueFactory);
            _readLock = new object();
            _writeLock = new object();
            _writer = new RedisWriter(_encoding);
            _asyncConnectArgs = new Lazy<SocketAsyncEventArgs>(SocketAsyncConnectFactory);
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
            if (!_asyncConnectionStarted && !IsConnected)
            {
                lock (_asyncConnectArgs)
                {
                    if (!_asyncConnectionStarted && !IsConnected)
                    {
                        _asyncConnectionStarted = true;
                        InitSocket();
                        if (!_socket.ConnectAsync(AsyncConnectArgs))
                            OnSocketConnected(AsyncConnectArgs);
                    }
                }
            }

            return _connectionTaskSource.Task;
        }

        public T Call<T>(RedisCommand<T> command)
        {
            ConnectIfNotConnected();

            try
            {
                if (IsPipelined)
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
            AsyncWriteQueue.Enqueue(token);
            ConnectAsync().ContinueWith(CallAsyncDeferred);
            return token.TaskSource.Task;
        }

        public void Write(RedisCommand command)
        {
            try
            {
                _writer.Write(command, _stream);
            }
            catch (IOException)
            {
                if (ReconnectAttempts == 0)
                    throw;
                Reconnect();
                Write(command);
            }
        }

        public T Read<T>(Func<RedisReader, T> func)
        {
            try
            {
                return func(_reader);
            }
            catch (IOException)
            {
                if (ReconnectAttempts == 0)
                    throw;
                Reconnect();
                return Read(func);
            }
        }

        public void Read(Stream destination, int bufferSize) // TODO: reconnect
        {
            try
            {
                _reader.ExpectType(RedisMessage.Bulk);
                _reader.ReadBulk(destination, bufferSize, false);
            }
            catch (IOException)
            {
                if (ReconnectAttempts == 0)
                    throw;
                Reconnect();
                Read(destination, bufferSize);
            }
        }

        public void BeginPipe()
        {
            ConnectIfNotConnected();
            _pipeline.Begin();
        }

        public object[] EndPipe()
        {
            try
            {
                return _pipeline.Flush();
            }
            catch (IOException)
            {
                if (ReconnectAttempts == 0)
                    throw;
                Reconnect();
                return EndPipe();
            }
        }

        public void Dispose()
        {
            if (_asyncConnectArgs.IsValueCreated)
                _asyncConnectArgs.Value.Dispose();

            if (_asyncTransferPool.IsValueCreated)
                _asyncTransferPool.Value.Dispose();

            if (_pipeline != null)
                _pipeline.Dispose();

            if (_stream != null)
                _stream.Dispose();

            if (_socket != null)
                _socket.Dispose();

        }

        void CallAsyncDeferred(Task t)
        {
            lock (_writeLock)
            {
                IRedisAsyncCommandToken token;
                if (!AsyncWriteQueue.TryDequeue(out token))
                    throw new Exception();

                AsyncReadQueue.Enqueue(token);

                var args = AsyncTransferPool.Acquire();
                int bytes;
                try
                {
                    bytes = _writer.Write(token.Command, args.Buffer, args.Offset);
                }
                catch (ArgumentException e)
                {
                    throw new RedisClientException("Could not write command '" + token.Command.Command + "'. Argument size exceeds buffer allocation of " + args.Count + ".", e);
                }
                args.SetBuffer(args.Offset, bytes);

                if (!_socket.SendAsync(args))
                    OnSocketSent(args);
            }
        }

        void ConnectIfNotConnected()
        {
            if (!IsConnected)
                Connect();
        }

        void Reconnect()
        {
            int attempts = 0;
            while (attempts++ < ReconnectAttempts || ReconnectAttempts == -1)
            {
                if (Connect())
                    return;

                Thread.Sleep(TimeSpan.FromMilliseconds(ReconnectWait));
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

        }

        void OnSocketSent(SocketAsyncEventArgs args)
        {
            AsyncTransferPool.Release(args);

            IRedisAsyncCommandToken token;
            lock (_readLock)
            {
                if (AsyncReadQueue.TryDequeue(out token))
                {
                    try
                    {
                        token.SetResult(_reader);
                    }
                    catch (IOException)
                    {
                        if (ReconnectAttempts == 0)
                            throw;
                        Reconnect();
                        AsyncWriteQueue.Enqueue(token);
                        ConnectAsync().ContinueWith(CallAsyncDeferred);
                    }
                    catch (Exception e)
                    {
                        token.SetException(e);
                    }
                }
            }
        }

        void OnConnected()
        {
            if (Connected != null)
                Connected(this, new EventArgs());
        }

        void InitSocket()
        {
            if (_socket != null)
                _socket.Dispose();
            if (_connectionTaskSource != null)
                _connectionTaskSource.TrySetResult(false);

            _connectionTaskSource = new TaskCompletionSource<bool>();
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }

        void InitReader()
        {
            if (_stream != null)
                _stream.Dispose();

            _stream = new BufferedStream(new NetworkStream(_socket));
            _reader = new RedisReader (_encoding, _stream);
            _pipeline = new RedisPipeline(_stream, _encoding, _reader);
            _connectionTaskSource.SetResult(_socket.Connected);
            OnConnected();
        }

        SocketAsyncPool SocketAsyncPoolFactory()
        {
            SocketAsyncPool pool = new SocketAsyncPool(_concurrency, _bufferSize);
            pool.Completed += OnSocketCompleted;
            return pool;
        }

        ConcurrentQueue<IRedisAsyncCommandToken> AsyncQueueFactory()
        {
            return new ConcurrentQueue<IRedisAsyncCommandToken>();
        }

        SocketAsyncEventArgs SocketAsyncConnectFactory()
        {
            SocketAsyncEventArgs args = new SocketAsyncEventArgs { RemoteEndPoint = _endpoint };
            args.Completed += OnSocketCompleted;
            return args;
        }
    }
}
