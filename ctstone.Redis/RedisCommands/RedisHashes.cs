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
            object[] response = RedisReader.ReadMultiBulk(stream);

            Dictionary<string, string>[] dicts = new Dictionary<string, string>[response.Length];
            for (int i = 0; i < response.Length; i++)
            {
                object[] hash = response[i] as object[];
                dicts[i] = HashMapper.GetDict(hash);
            }
            return dicts;
        }
    }

    class RedisHashes<T> : RedisCommand<T[]>
        where T : new()
    {
        public RedisHashes(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static T[] ParseStream(Stream stream)
        {
            object[] response = RedisReader.ReadMultiBulk(stream);

            T[] objs = new T[response.Length];
            for (int i = 0; i < response.Length; i++)
            {
                object[] hash = response[i] as object[];
                objs[i] = HashMapper.ReflectHash<T>(hash);
            }
            return objs;
        }
    }
}
