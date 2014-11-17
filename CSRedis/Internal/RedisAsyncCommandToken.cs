using CSRedis.Internal.IO;
using System;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    interface IRedisAsyncCommandToken
    {
        RedisCommand Command { get; }
        void SetResult(RedisReader reader);
        void SetException(Exception e);
        bool TrySetException(Exception e);
    }

    class RedisAsyncCommandToken<T> : TaskCompletionSource<T>, IRedisAsyncCommandToken
    {
        readonly RedisCommand<T> _command;

        public RedisCommand Command { get { return _command; } }

        public RedisAsyncCommandToken(RedisCommand<T> command)
        {
            _command = command;
        }

        public void SetResult(RedisReader reader)
        {
            SetResult(_command.Parse(reader));
        }
    }
}
