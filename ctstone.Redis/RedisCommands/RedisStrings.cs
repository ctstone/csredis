
namespace ctstone.Redis.RedisCommands
{
    class RedisStrings : RedisCommand<string[]>
    {
        public RedisStrings(string command, params object[] args)
            : base(RedisReader.ReadMultiBulkUTF8, command, args)
        { }
    }
}
