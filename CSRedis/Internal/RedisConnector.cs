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
        readonly SocketAsyncEventArgs _connectArgs; // TODO: lazy
        readonly SocketAsyncPool _transferPool; // TODO: lazy
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

            
            _connectArgs = new SocketAsyncEventArgs { RemoteEndPoint = _endpoint };
            _connectArgs.Completed += OnSocketCompleted;
            _transferPool.Completed += OnSocketCompleted;
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
                lock (_connectArgs)
                {
                    if (!_asyncConnectionStarted && !IsConnected)
                    {
                        _asyncConnectionStarted = true;
                        InitSocket();
                        if (!_socket.ConnectAsync(_connectArgs))
                            OnSocketConnected(_connectArgs);
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
            _writeQueue.Enqueue(token);
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
            _connectArgs.Dispose();
            _transferPool.Dispose();

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
                if (!_writeQueue.TryDequeue(out token))
                    throw new Exception();

                _readQueue.Enqueue(token);

                var args = _transferPool.Acquire();
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
            _transferPool.Release(args);

            IRedisAsyncCommandToken token;
            lock (_readLock)
            {
                if (_readQueue.TryDequeue(out token))
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
                        _writeQueue.Enqueue(token);
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

        
    }
}
