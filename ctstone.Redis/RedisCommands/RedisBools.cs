using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisBools : RedisCommand<bool[]>
    {
        public RedisBools(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static bool[] ParseStream(Stream s)
        {
            object[] response = RedisReader.ReadMultiBulk(s);
            bool[] bools = new bool[response.Length];
            for (int i = 0; i < response.Length; i++)
                bools[i] = (long)response[i] == 1;
            return bools;
        }
    }
}
