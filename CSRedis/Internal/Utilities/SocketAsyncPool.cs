using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace CSRedis.Internal
{
    class SocketAsyncPool
    {
        readonly byte[] _buffer;
        readonly Stack<SocketAsyncEventArgs> _pool;
        readonly int _bufferSize;
        readonly Semaphore _acquisitionGate;

        public event EventHandler<SocketAsyncEventArgs> Completed;

        public SocketAsyncPool(int concurrency, int bufferSize)
        {
            _pool = new Stack<SocketAsyncEventArgs>();
            _bufferSize = bufferSize;
            _buffer = new byte[concurrency * bufferSize];
            _acquisitionGate = new Semaphore(concurrency, concurrency);
            for (int i = 0; i < _buffer.Length; i += bufferSize)
            {
                SocketAsyncEventArgs args = new SocketAsyncEventArgs();
                args.Completed += OnSocketCompleted;
                args.SetBuffer(_buffer, i, bufferSize);
                _pool.Push(args);
            }
        }

        public SocketAsyncEventArgs Acquire()
        {
            if (!_acquisitionGate.WaitOne())
                throw new Exception();

            lock (_pool)
            {
                var args = _pool.Pop();  // TODO: check for empty pool
                args.SetBuffer(args.Offset, _bufferSize);
                return args;
            }
        }

        public void Release(SocketAsyncEventArgs args)
        {
            lock (_pool)
            {
                if (args.Buffer.Equals(_buffer))
                {
                    _pool.Push(args);
                }
            }
            _acquisitionGate.Release();
        }

        void OnSocketCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (Completed != null)
                Completed(sender, e);
        }
    }
}
