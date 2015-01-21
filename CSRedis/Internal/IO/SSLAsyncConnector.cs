using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    class SSLAsyncConnector : IAsyncConnector
    {
        readonly IRedisSocket _socket;
        readonly RedisIO _io;
        readonly EndPoint _endPoint;
        readonly IOQueue _ioQueue;
        readonly object _readLock;
        readonly object _writeLock;
        TaskCompletionSource<bool> _connected;
        bool _connecting;

        public event EventHandler Connected;

        public SSLAsyncConnector(IRedisSocket socket, EndPoint endPoint, RedisIO io)
        {
            _socket = socket;
            _io = io;
            _endPoint = endPoint;
            _ioQueue = new IOQueue();
            _writeLock = new object();
            _readLock = new object();
            _connected = new TaskCompletionSource<bool>();
        }

        public Task<bool> ConnectAsync()
        {
            if (_connecting)
                return _connected.Task;

            _connecting = true;

            return _socket.ConnectAsync().ContinueWith(t =>
            {
                if (Connected != null)
                    Connected(this, new EventArgs());
                _connected.SetResult(t.Result);
                return t.Result;
            });

        }

        public Task<T> CallAsync<T>(RedisCommand<T> command)
        {
            var token = new RedisAsyncCommandToken<T>(command);
            _ioQueue.Enqueue(token);
            ConnectAsync().ContinueWith(CallAsync_Continued);
            return token.TaskSource.Task;
        }

        public void Dispose()
        { }

        void CallAsync_Continued(Task t)
        {
            lock (_writeLock)
            {
                IRedisAsyncCommandToken token = _ioQueue.DequeueForWrite();
                _io.Writer.WriteAsync(token.Command, _io.Stream).ContinueWith(WriteAsync_Continued);
            }
        }

        void WriteAsync_Continued(Task t)
        {
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
    }
}
