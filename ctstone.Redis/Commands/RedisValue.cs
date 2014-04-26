using ctstone.Redis.IO;
using System.IO;

namespace ctstone.Redis.Commands
{
    class RedisValue : RedisCommand<string>
    {
        public RedisValue(string command, params object[] args)
            : base(ParseStream, command, args)
        { }
        private static string ParseStream(Stream stream)
        {
            string[] result = RedisReader.ReadMultiBulkString(stream);
            if (result == null)
                return null;
            return result[1];
        }
    }
}
