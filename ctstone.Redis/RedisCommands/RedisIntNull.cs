using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisIntNull : RedisCommand<long?>
    {
        public RedisIntNull(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static long? ParseStream(Stream stream)
        {
            RedisMessage type = RedisReader.ReadType(stream);
            if (type == RedisMessage.Int)
                return RedisReader.ReadInt(stream, false);

            RedisReader.ReadBulkUTF8(stream, false);
            return null;
        }
    }
}
