using System;
using System.Globalization;
using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisFloat : RedisCommand<double>
    {
        public RedisFloat(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static double ParseStream(Stream stream)
        {
            return Double.Parse(RedisReader.ReadBulkUTF8(stream), NumberStyles.Float);
        }
    }

    class RedisFloatNull : RedisCommand<double?>
    {
        public RedisFloatNull(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static double? ParseStream(Stream stream)
        {
            string result = RedisReader.ReadBulkUTF8(stream);
            if (result == null)
                return null;
            return Double.Parse(result, NumberStyles.Float);
        }
    }
}
