
namespace ctstone.Redis.RedisCommands
{
    class RedisBytes : RedisCommand<byte[]>
    {
        public RedisBytes(string command, params object[] args)
            : base(RedisReader.ReadBulk, command, args)
        { }
    }
}
