using System;
using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisDateMicro : RedisCommand<DateTime>
    {
        public RedisDateMicro(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static DateTime ParseStream(Stream stream)
        {
            string[] parts = RedisReader.ReadMultiBulkUTF8(stream);
            int timestamp = Int32.Parse(parts[0]);
            int microseconds = Int32.Parse(parts[1]);
            long ticks = microseconds * (TimeSpan.TicksPerMillisecond / 1000);
            return RedisArgs.Epoch
                + TimeSpan.FromSeconds(timestamp)
                + TimeSpan.FromTicks(ticks);
        }
    }
}
