using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ctstone.Redis.RedisCommands
{
    class RedisScanCommand : RedisCommand<RedisScan>
    {
        public RedisScanCommand(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        static RedisScan ParseStream(Stream stream)
        {
            RedisReader.ExpectType(stream, RedisMessage.MultiBulk);

            long count = RedisReader.ReadInt(stream, false);
            RedisScan scan = new RedisScan();
            scan.Cursor = Int64.Parse(RedisReader.Read(stream).ToString());
            RedisReader.ExpectType(stream, RedisMessage.MultiBulk);

            scan.Items = new string[RedisReader.ReadInt(stream, false)];
            for (int i = 0; i < scan.Items.Length; i++)
                scan.Items[i] = RedisReader.Read(stream).ToString();
            return scan;
        }

        static long ParseCursor(object obj)
        {
            return Int64.Parse(obj as String);
        }
    }

    class RedisScanPairCommand : RedisCommand<RedisScanPair>
    {
        public RedisScanPairCommand(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        static RedisScanPair ParseStream(Stream stream)
        {
            object[] parts = RedisReader.ReadMultiBulk(stream);
            object[] values = parts[1] as object[];
            RedisScanPair result = new RedisScanPair
            {
                Cursor = ParseCursor(parts[0]),
                Items = new Dictionary<string,object>(),
            };
            for (int i = 0; i < values.Length; i += 2)
                result.Items[values[i] as String] = values[i + 1];

            return result;
        }

        static long ParseCursor(object obj)
        {
            return Int64.Parse(obj as String);
        }
    }

    public class RedisScan
    {
        public long Cursor { get; set; }
        public string[] Items { get; set; }
    }

    public class RedisScanPair
    {
        public long Cursor { get; set; }
        public Dictionary<string, object> Items { get; set; }
    }
}
