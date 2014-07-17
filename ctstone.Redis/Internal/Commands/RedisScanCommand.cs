using ctstone.Redis.Internal.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ctstone.Redis.Internal.Commands
{
    class RedisScanCommand<T>: RedisCommand<RedisScan<T>>
    {
        RedisCommand<T[]> _command;

        public RedisScanCommand(RedisCommand<T[]> command)
            : base(command.Command, command.Arguments)
        {
            _command = command;
        }

        public override RedisScan<T> Parse(RedisReader reader)
        {
            RedisScan<T> scan = new RedisScan<T>();

            reader.ExpectType(RedisMessage.MultiBulk);
            if (reader.ReadInt(false) != 2)
                throw new RedisProtocolException("Expected 2 items");

            scan.Cursor = Int64.Parse(reader.ReadBulkString());
            scan.Items = _command.Parse(reader);

            return scan;
        }
    }
}
