using System.Collections.Generic;
using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisHashes : RedisCommand<Dictionary<string, string>[]>
    {
        public RedisHashes(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static Dictionary<string, string>[] ParseStream(Stream stream)
        {
            object[] result = RedisReader.ReadMultiBulk(stream);
            Dictionary<string, string>[] dicts = new Dictionary<string, string>[result.Length];
            for (int i = 0; i < result.Length; i++)
            {
                object[] array = result[i] as object[];
                dicts[i] = new Dictionary<string, string>();
                for (int j = 0; j < array.Length; j += 2)
                {
                    dicts[i][array[j].ToString()] = array[j + 1].ToString();
                }
            }
            return dicts;
        }
    }
}
