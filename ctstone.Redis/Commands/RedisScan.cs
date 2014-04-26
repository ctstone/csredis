using ctstone.Redis.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ctstone.Redis.Commands
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

    /// <summary>
    /// Represents the result of a Redis SCAN or SSCAN operation
    /// </summary>
    public class RedisScan
    {
        /// <summary>
        /// Updated cursor that should be used as the cursor argument in the next call
        /// </summary>
        public long Cursor { get; set; }
        /// <summary>
        /// Collection of elements returned by the SCAN operation
        /// </summary>
        public string[] Items { get; set; }
    }

    /// <summary>
    /// Represents the result of a Redis HSCAN or ZSCAN operation
    /// </summary>
    public class RedisScanPair
    {
        /// <summary>
        /// Updated cursor that should be used as the cursor argument in the next call
        /// </summary>
        public long Cursor { get; set; }
        /// <summary>
        /// Collection of elements returned by the SCAN operation
        /// </summary>
        public Dictionary<string, object> Items { get; set; }
    }
}
