
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisStrings : RedisCommand<string[]>
    {
        public RedisStrings(string command, params object[] args)
            : base(RedisReader.ReadMultiBulkString, command, args)
        { }
    }
}
