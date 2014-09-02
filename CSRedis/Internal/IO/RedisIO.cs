using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CSRedis.Internal.IO
{
    class RedisIO : IDisposable
    {
        readonly RedisWriter _writer;
        RedisReader _reader;
        RedisPipeline _pipeline;
        BufferedStream _stream;

        public RedisWriter Writer { get { return _writer; } }
        public RedisReader Reader { get { return _reader; } }
        public Encoding Encoding { get; set; }
        public RedisPipeline Pipeline { get { return _pipeline; } }
        public Stream Stream { get { return _stream; } }

        public RedisIO()
        {
            _writer = new RedisWriter(this);
            Encoding = Encoding.UTF8;
        }

        public void SetStream(Stream stream)
        {
            if (_stream != null)
                _stream.Dispose();

            _stream = new BufferedStream(stream);
            _reader = new RedisReader(this);
            _pipeline = new RedisPipeline(this);
        }

        public void Dispose()
        {
            if (_pipeline != null)
                _pipeline.Dispose();
            if (_stream != null)
                _stream.Dispose();
        }
    }
}
