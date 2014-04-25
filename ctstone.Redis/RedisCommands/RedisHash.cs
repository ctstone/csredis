using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;

namespace ctstone.Redis.RedisCommands
{
    class RedisHash : RedisCommand<Dictionary<string, string>>
    {
        public RedisHash(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static Dictionary<string, string> ParseStream(Stream stream)
        {
            string[] fieldValues = RedisReader.ReadMultiBulkString(stream);
            return HashMapper.ToDict(fieldValues);
        }
    }

    class RedisHash<T> : RedisCommand<T>
        where T : class
    {
        public RedisHash(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static T ParseStream(Stream stream)
        {
            string[] fieldValues = RedisReader.ReadMultiBulkString(stream);
            return HashMapper.ToObject<T>(fieldValues);
        }
    }
}
