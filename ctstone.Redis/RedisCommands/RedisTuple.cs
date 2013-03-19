using System;
using System.ComponentModel;
using System.IO;

namespace ctstone.Redis.RedisCommands
{
    class RedisTuple : RedisCommand<Tuple<string, string>>
    {
        public RedisTuple(string command, params object[] args)
            : base(ParseStream, command, args)
        { }
        protected static Tuple<string, string> ParseStream(Stream stream)
        {
            string[] result = RedisReader.ReadMultiBulkUTF8(stream);
            if (result == null)
                return null;
            return Tuple.Create(result[0], result[1]);
        }
    }

    class RedisTuple<Item1, Item2> : RedisCommand<Tuple<Item1, Item2>>
    {
        public RedisTuple(string command, params object[] args)
            : base(ParseStream, command, args)
        { }
        protected static Tuple<Item1, Item2> ParseStream(Stream stream)
        {
            string[] result = RedisReader.ReadMultiBulkUTF8(stream);
            if (result == null)
                return null;

            TypeConverter tc_key = TypeDescriptor.GetConverter(typeof(Item1));
            TypeConverter tc_val = TypeDescriptor.GetConverter(typeof(Item2));
            Item1 key = (Item1)tc_key.ConvertFrom(result[0]);
            Item2 value = (Item2)tc_val.ConvertFrom(result[1]);
            return Tuple.Create(key, value);
        }
    }
}
