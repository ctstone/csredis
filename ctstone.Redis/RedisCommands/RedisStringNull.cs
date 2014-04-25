using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisStringNull : RedisCommand<string>
    {
        public RedisStringNull(string command, params object[] args)
            : base(Read, command, args)
        { }

        private static string Read(Stream stream)
        {
            var type = RedisReader.ReadType(stream);
            if (type == RedisMessage.Bulk)
                return RedisReader.ReadBulkString(stream, false);
            RedisReader.ReadMultiBulk(stream, false);
            return null;
        }
    }
}
