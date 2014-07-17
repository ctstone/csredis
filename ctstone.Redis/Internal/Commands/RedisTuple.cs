using ctstone.Redis.Internal.IO;
using System;
using System.ComponentModel;
using System.IO;

namespace ctstone.Redis.Internal.Commands
{
    class RedisTuple : RedisCommand<Tuple<string, string>>
    {
        public RedisTuple(string command, params object[] args)
            : base(command, args)
        { }

        public override Tuple<string, string> Parse(RedisReader reader)
        {
            reader.ExpectType(RedisMessage.MultiBulk);
            long count = reader.ReadInt(false);
            if (count != 2)
                throw new RedisProtocolException("Expected 2 items");
            return Tuple.Create(reader.ReadBulkString(), reader.ReadBulkString());
        }

        public class Generic<T1, T2>
        {
            static Lazy<TypeConverter> converter1
                = new Lazy<TypeConverter>(() => TypeDescriptor.GetConverter(typeof(T1)));
            static Lazy<TypeConverter> converter2
                = new Lazy<TypeConverter>(() => TypeDescriptor.GetConverter(typeof(T2)));

            private Generic() { }

            public class Bulk : RedisCommand<Tuple<T1, T2>>
            {
                public Bulk(string command, params object[] args)
                    : base(command, args)
                { }

                public override Tuple<T1, T2> Parse(RedisReader reader)
                {
                    return Create(reader);
                }
            }

            public class MultiBulk : RedisCommand<Tuple<T1, T2>>
            {
                public MultiBulk(string command, params object[] args)
                    : base(command, args)
                { }

                public override Tuple<T1, T2> Parse(RedisReader reader)
                {
                    reader.ExpectType(RedisMessage.MultiBulk);
                    long count = reader.ReadInt(false);
                    if (count != 2)
                        throw new RedisProtocolException("Expected 2 items");

                    return Create(reader);
                }
            }

            static Tuple<T1, T2> Create(RedisReader reader)
            {
                T1 item1 = (T1)converter1.Value.ConvertFrom(reader.ReadBulkString());
                T2 item2 = (T2)converter2.Value.ConvertFrom(reader.ReadBulkString());

                return Tuple.Create(item1, item2);
            }
        }
    }
}
