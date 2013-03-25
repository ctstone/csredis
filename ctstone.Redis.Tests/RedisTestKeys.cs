using ctstone.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis.Tests.RedisClientTests
{
    class RedisTestKeys : IDisposable
    {
        private RedisClient _redis;
        private RedisClientAsync _async;
        private string[] _keys;

        public RedisTestKeys(RedisClient redis, params string[] keys)
        {
            _redis = redis;
            _keys = keys;
            _redis.Del(keys);
        }

        public RedisTestKeys(RedisClientAsync async, params string[] keys)
        {
            _async = async;
            _keys = keys;
            _async.Del(keys).Wait();
        }

        public void Dispose()
        {
            if (_redis != null)
                _redis.Del(_keys);
            if (_async != null)
                _async.Del(_keys).Wait();
        }
    }
}
