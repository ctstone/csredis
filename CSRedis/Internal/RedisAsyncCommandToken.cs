using CSRedis.Internal.IO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    interface IRedisAsyncCommandToken
    {
        RedisCommand Command { get; }
        void SetResult(RedisReader reader);
    }

    class RedisAsyncCommandToken<T> : IRedisAsyncCommandToken
    {
        readonly TaskCompletionSource<T> _tcs;
        readonly RedisCommand<T> _command;

        public TaskCompletionSource<T> TaskSource { get { return _tcs; } }
        public RedisCommand Command { get { return _command; } }

        public RedisAsyncCommandToken(RedisCommand<T> command)
        {
            _tcs = new TaskCompletionSource<T>();
            _command = command;
        }

        public void SetResult(RedisReader reader)
        {
            try
            {
                _tcs.SetResult(_command.Parse(reader));
            }
            catch (Exception e)
            {
                _tcs.SetException(e);
            }
        }
    }
}
