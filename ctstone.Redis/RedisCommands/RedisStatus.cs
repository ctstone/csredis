
using System.IO;
namespace ctstone.Redis.RedisCommands
{
    class RedisStatus : RedisCommand<string>
    {
        public RedisStatus(string command, params object[] args)
            : base(RedisReader.ReadStatus, command, args)
        { }
    }

    class RedisStatusNull : RedisCommand<string>
    {
        public RedisStatusNull(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static string ParseStream(Stream stream)
        {
            RedisMessage type = RedisReader.ReadType(stream);
            if (type == RedisMessage.Status)
                return RedisReader.ReadStatus(stream, false);

            object[] result = RedisReader.ReadMultiBulk(stream, false);
            if (result != null)
                throw new RedisProtocolException("Expecting null MULTI BULK response. Received: " + result.ToString());
            return null;
        }
    }
}
