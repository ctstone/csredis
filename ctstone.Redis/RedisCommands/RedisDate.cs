using System;
using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisDate : RedisCommand<DateTime>
    {
        public RedisDate(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static DateTime ParseStream(Stream stream)
        {
            long timestamp = RedisReader.ReadInt(stream);
            return RedisArgs.Epoch + TimeSpan.FromSeconds(timestamp);
        }
    }
}
