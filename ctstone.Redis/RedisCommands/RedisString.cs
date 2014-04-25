
namespace ctstone.Redis.RedisCommands
{
    class RedisString : RedisCommand<string>
    {
        public RedisString(string command, params object[] args)
            : base(RedisReader.ReadBulkString, command, args)
        { }
    }
}
