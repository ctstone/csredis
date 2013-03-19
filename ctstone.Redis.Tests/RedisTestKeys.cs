using ctstone.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis.Tests
{
    class RedisTestKeys : IDisposable
    {
        private RedisClient _redis;
        private string[] _keys;

        public RedisTestKeys(RedisClient redis, params string[] keys)
        {
            _redis = redis;
            _keys = keys;
            _redis.Del(keys);
        }

        public void Dispose()
        {
            _redis.Del(_keys);
        }
    }
}
