using ctstone.Redis.IO;
using System.IO;

namespace ctstone.Redis.Commands
{
    class RedisBool : RedisCommand<bool>
    {
        public RedisBool(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static bool ParseStream(Stream stream)
        {
            return RedisReader.ReadInt(stream) == 1;
        }
    }
}
