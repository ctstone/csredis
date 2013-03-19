
namespace ctstone.Redis.RedisCommands
{
    class RedisStatus : RedisCommand<string>
    {
        public RedisStatus(string command, params object[] args)
            : base(RedisReader.ReadStatus, command, args)
        { }
    }
}
