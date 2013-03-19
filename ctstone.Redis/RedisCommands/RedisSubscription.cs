using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisSubscription : RedisCommand<RedisSubscriptionResponse>
    {
        public RedisSubscription(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static RedisSubscriptionResponse ParseStream(Stream stream)
        {
            return RedisSubscriptionResponse.ReadResponse(RedisReader.ReadMultiBulk(stream));
        }
    }
}
