using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CSRedis.Internal.IO
{
    class IOQueue
    {
        readonly ConcurrentQueue<IRedisAsyncCommandToken> _readQueue;
        readonly ConcurrentQueue<IRedisAsyncCommandToken> _writeQueue;

        public IOQueue()
        {
            _readQueue = new ConcurrentQueue<IRedisAsyncCommandToken>();
            _writeQueue = new ConcurrentQueue<IRedisAsyncCommandToken>();
        }

        public void Enqueue(IRedisAsyncCommandToken token)
        {
            _writeQueue.Enqueue(token);
        }

        public IRedisAsyncCommandToken DequeueForWrite()
        {
            IRedisAsyncCommandToken token;
            if (!_writeQueue.TryDequeue(out token))
                throw new Exception();
            _readQueue.Enqueue(token);
            return token;
        }

        public IRedisAsyncCommandToken DequeueForRead()
        {
            IRedisAsyncCommandToken token;
            if (!_readQueue.TryDequeue(out token))
                throw new Exception();
            return token;
        }
    }
}
