
using ctstone.Redis.Internal.IO;
using ctstone.Redis.Internal.Utilities;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
namespace ctstone.Redis.Internal.Commands
{
    class RedisStatus : RedisCommand<string>
    {
        public RedisStatus(string command, params object[] args)
            : base(command, args)
        { }

        public override string Parse(RedisReader reader)
        {
            return reader.ReadStatus();
        }

        public class Empty : RedisCommand<string>
        {
            public Empty(string command, params object[] args)
                : base(command, args)
            { }
            public override string Parse(RedisReader reader)
            {
                RedisMessage type = reader.ReadType();
                if ((int)type == -1)
                    return String.Empty;
                else if (type == RedisMessage.Error)
                    throw new RedisException(reader.ReadStatus(false));

                throw new RedisProtocolException("Unexpected type: " + type);
            }
        }

        public class Nullable : RedisCommand<string>
        {
            public Nullable(string command, params object[] args)
                : base(command, args)
            { }

            public override string Parse(RedisReader reader)
            {
                RedisMessage type = reader.ReadType();
                if (type == RedisMessage.Status)
                    return reader.ReadStatus(false);

                object[] result = reader.ReadMultiBulk(false);
                if (result != null)
                    throw new RedisProtocolException("Expecting null MULTI BULK response. Received: " + result.ToString());

                return null;
            }
        }
    }
}
