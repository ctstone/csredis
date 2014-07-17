using ctstone.Redis.Internal.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ctstone.Redis.Internal
{
    class RedisPipeline : IDisposable
    {
        readonly Stream _buffer;
        readonly Stream _destination;
        readonly RedisWriter _writer;
        readonly RedisReader _reader;
        readonly Queue<Func<object>> _parsers;

        public bool Active { get; set; }

        internal RedisPipeline(Stream destination, Encoding encoding, RedisReader reader)
        {
            _reader = reader;
            _destination = destination;
            _buffer = new MemoryStream();
            _writer = new RedisWriter(_buffer, encoding);
            _parsers = new Queue<Func<object>>();
        }

        public void Write<T>(RedisCommand<T> command)
        {
            _writer.Write(command.Command, command.Arguments);
            _parsers.Enqueue(() => command.Parse(_reader));
        }

        public object[] Flush()
        {
            _buffer.Position = 0;
            _buffer.CopyTo(_destination);

            object[] results = new object[_parsers.Count];
            for (int i = 0; i < results.Length; i++)
                results[i] = _parsers.Dequeue()();
            _buffer.SetLength(0);
            return results;
        }

        public void Dispose()
        {
            _buffer.Dispose();
            _writer.Dispose();
        }
    }
}
