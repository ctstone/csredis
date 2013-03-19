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
            Dictionary<string, string> dict = new Dictionary<string, string>();
            string[] hash = RedisReader.ReadMultiBulkUTF8(stream);
            if (hash.Length == 0)
                return dict;
            string field, value;
            int i;
            for (i = 0; i < hash.Length; i += 2)
            {
                field = hash[i];
                value = hash[i + 1];
                dict[field] = value;
            }
            return dict;
        }
    }

    class RedisHashCommand<T> : RedisCommand<T>
        where T : new()
    {
        public RedisHashCommand(string command, params object[] args)
            : base(ParseStream, command, args)
        { }

        private static T ParseStream(Stream stream)
        {
            return ReflectHash(RedisReader.ReadMultiBulkUTF8(stream));
        }

        private static T ReflectHash(string[] fields, string[] values)
        {
            List<string> hash = new List<string>();
            for (int i = 0; i < fields.Length; i++)
            {
                hash.AddRange(new[] { fields[i], values[i] });
            }
            return ReflectHash(hash.ToArray());
        }

        private static T ReflectHash(string[] hash)
        {
            T obj = Activator.CreateInstance<T>();
            if (hash.Length == 0)
                return obj;
            TypeConverter conv;
            PropertyInfo prop;
            string field, value;
            for (int i = 0; i < hash.Length; i += 2)
            {
                field = hash[i];
                value = hash[i + 1];
                prop = typeof(T).GetProperty(field, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                if (prop == null || !prop.CanWrite)
                    continue;
                conv = TypeDescriptor.GetConverter(prop.PropertyType);
                if (!conv.CanConvertFrom(typeof(String)))
                    continue;
                prop.SetValue(obj, conv.ConvertFrom(value), null);
            }

            return obj;
        }
    }
}
