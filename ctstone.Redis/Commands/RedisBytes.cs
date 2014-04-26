
using ctstone.Redis.IO;
namespace ctstone.Redis.Commands
{
    class RedisBytes : RedisCommand<byte[]>
    {
        public RedisBytes(string command, params object[] args)
            : base(RedisReader.ReadBulk, command, args)
        { }
    }
}
