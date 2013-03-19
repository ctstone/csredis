using System.IO;

namespace ctstone.Redis.RedisCommands
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
