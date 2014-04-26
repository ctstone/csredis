
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisInt : RedisCommand<long>
    {
        public RedisInt(string command, params object[] args)
            : base(RedisReader.ReadInt, command, args)
        { }
    }
}
