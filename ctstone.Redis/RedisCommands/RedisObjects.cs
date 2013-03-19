
namespace ctstone.Redis.RedisCommands
{
    class RedisObjects : RedisCommand<object[]>
    {
        public RedisObjects(string command, params object[] args)
            : base(RedisReader.ReadMultiBulk, command, args)
        { }
    }
}
