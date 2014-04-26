
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisObjects : RedisCommand<object[]>
    {
        public RedisObjects(string command, params object[] args)
            : base(RedisReader.ReadMultiBulk, command, args)
        { }
    }
}
