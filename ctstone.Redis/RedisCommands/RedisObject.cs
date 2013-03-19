
namespace ctstone.Redis.RedisCommands
{
    class RedisObject : RedisCommand<object>
    {
        public RedisObject(string command, params object[] args)
            : base(RedisReader.Read, command, args)
        { }
    }
}
