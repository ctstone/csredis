using ctstone.Redis.IO;
using ctstone.Redis.Utilities;
using System.Collections.Generic;
using System.IO;

namespace ctstone.Redis.Commands
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
                dicts[i] = HashMapper.ToDict(hash);
            }
            return dicts;
        }
    }

    class RedisStringHashes : RedisCommand<Dictionary<string, string>[]>
    {
        private string[] _fields;

        public RedisStringHashes(string command, string[] fields, params object[] args)
            : base(x => ParseStream(x, fields), command, args)
        {
            _fields = fields;
        }

        private static Dictionary<string, string>[] ParseStream(Stream stream, string[] fields)
        {
            string[] response = RedisReader.ReadMultiBulkString(stream);

            Dictionary<string, string>[] dicts = new Dictionary<string, string>[response.Length / fields.Length];
            for (int i = 0; i < response.Length; i += fields.Length)
            {
                dicts[i / fields.Length] = new Dictionary<string, string>();
                for (int j = 0; j < fields.Length; j++)
                    dicts[i / fields.Length][fields[j]] = response[i + j];
            }
            return dicts;
        }
    }

    class RedisHashes<T> : RedisCommand<T[]>
        where T : class
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
                objs[i] = HashMapper.ToObject<T>(hash);
            }
            return objs;
        }
    }
}
