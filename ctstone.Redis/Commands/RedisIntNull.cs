using ctstone.Redis.IO;
using System.IO;

namespace ctstone.Redis.Commands
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

            RedisReader.ReadBulkString(stream, false);
            return null;
        }
    }
}
