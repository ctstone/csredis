
using CSRedis.Internal.IO;
namespace CSRedis.Internal.Commands
{
    class RedisString : RedisCommand<string>
    {
        public RedisString(string command, params object[] args)
            : base(command, args)
        { }

        public override string Parse(RedisReader reader)
        {
            return reader.ReadBulkString();
        }

        public class Nullable : RedisString
        {
            public Nullable(string command, params object[] args)
                : base(command, args)
            { }

            public override string Parse(RedisReader reader)
            {
                RedisMessage type = reader.ReadType();
                if (type == RedisMessage.Bulk)
                    return reader.ReadBulkString(false);
                reader.ReadMultiBulk(false);
                return null;
            }
        }
    }
}
