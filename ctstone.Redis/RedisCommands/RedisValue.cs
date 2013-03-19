using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisValue : RedisCommand<string>
    {
        public RedisValue(string command, params object[] args)
            : base(ParseStream, command, args)
        { }
        private static string ParseStream(Stream stream)
        {
            string[] result = RedisReader.ReadMultiBulkUTF8(stream);
            if (result == null)
                return null;
            return result[1];
        }
    }
}
