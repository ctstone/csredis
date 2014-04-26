
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisObject : RedisCommand<object>
    {
        public RedisObject(string command, params object[] args)
            : base(RedisReader.Read, command, args)
        { }
    }
}
