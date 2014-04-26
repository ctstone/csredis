
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisString : RedisCommand<string>
    {
        public RedisString(string command, params object[] args)
            : base(RedisReader.ReadBulkString, command, args)
        { }
    }
}
