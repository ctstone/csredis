
namespace ctstone.Redis.RedisCommands
{
    class RedisInt : RedisCommand<long>
    {
        public RedisInt(string command, params object[] args)
            : base(RedisReader.ReadInt, command, args)
        { }
    }
}
