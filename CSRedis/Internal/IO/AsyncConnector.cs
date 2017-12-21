using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    class AsyncConnector : IAsyncConnector
    {
        readonly SocketAsyncEventArgs _asyncConnectArgs;
        readonly SocketAsyncPool _asyncTransferPool;
        readonly IOQueue _ioQueue;
        readonly object _readLock;
        readonly object _writeLock;
        readonly int _concurrency;
        readonly int _bufferSize;
        readonly IRedisSocket _redisSocket;
        readonly RedisIO _io;

        bool _asyncConnectionStarted;
        TaskCompletionSource<bool> _connectionTaskSource;

        public event EventHandler Connected;


        public AsyncConnector(IRedisSocket socket, EndPoint endpoint, RedisIO io, int concurrency, int bufferSize)
        {
            _redisSocket = socket;
            _io = io;
            _concurrency = concurrency;
            _bufferSize = bufferSize;
            _asyncTransferPool = new SocketAsyncPool(concurrency, bufferSize);
            _asyncTransferPool.Completed += OnSocketCompleted;
            _ioQueue = new IOQueue();
            _readLock = new object();
            _writeLock = new object();
            _asyncConnectArgs = new SocketAsyncEventArgs { RemoteEndPoint = endpoint };
            _asyncConnectArgs.Completed += OnSocketCompleted;
            _connectionTaskSource = new TaskCompletionSource<bool>();
        }

        public Task<bool> ConnectAsync()
        {
            if (_redisSocket.Connected)
                _connectionTaskSource.SetResult(true);

            if (!_asyncConnectionStarted && !_redisSocket.Connected)
            {
                lock (_asyncConnectArgs)
                {
                    if (!_asyncConnectionStarted && !_redisSocket.Connected)
                    {
                        _asyncConnectionStarted = true;
                        if (!_redisSocket.ConnectAsync(_asyncConnectArgs))
                            OnSocketConnected(_asyncConnectArgs);
                    }
                }
            }

            return _connectionTaskSource.Task;
        }

        public Task<T> CallAsync<T>(RedisCommand<T> command)
        {
            var token = new RedisAsyncCommandToken<T>(command);
            _ioQueue.Enqueue(token);
            ConnectAsync().ContinueWith(CallAsync_Continued);
            return token.TaskSource.Task;
        }

        void InitConnection()
        {
            if (_connectionTaskSource != null)
                _connectionTaskSource.TrySetResult(false);

            _connectionTaskSource = new TaskCompletionSource<bool>();
        }

        void CallAsync_Continued(Task t)
        {
            lock (_writeLock)
            {
                IRedisAsyncCommandToken token = _ioQueue.DequeueForWrite();

                SocketAsyncEventArgs args = _asyncTransferPool.Acquire();
                int bytes = TryWriteBuffer(token.Command, args.Buffer, args.Offset);
                args.SetBuffer(args.Offset, bytes);
                if (!_redisSocket.SendAsync(args))
                    OnSocketSent(args);
            }
        }

        int TryWriteBuffer(RedisCommand command, byte[] buffer, int offset)
        {
            try
            {
                return _io.Writer.Write(command, buffer, offset);
            }
            catch (ArgumentException e)
            {
                throw new RedisClientException("Could not write command '" + command.Command + "'. Argument size exceeds buffer size.", e);
            }
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
            _connectionTaskSource.SetResult(_redisSocket.Connected);

            if (Connected != null)
                Connected(this, new EventArgs());
        }

        void OnSocketSent(SocketAsyncEventArgs args)
        {
            _asyncTransferPool.Release(args);

            lock (_readLock)
            {
                IRedisAsyncCommandToken token = _ioQueue.DequeueForRead();
                TrySetResult(token);
            }
        }

        void TrySetResult(IRedisAsyncCommandToken token)
        {
            try
            {
                token.SetResult(_io.Reader);
            }
            // TODO: catch IOException and reconnect
            catch (Exception e)
            {
                token.SetException(e);
            }
        }

        public void Dispose()
        {
            _asyncTransferPool.Dispose();
            _asyncConnectArgs.Dispose();
        }
    }
}
