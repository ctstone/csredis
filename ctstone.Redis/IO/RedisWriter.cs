using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ctstone.Redis.IO
{
    class RedisWriter
    {
        const char Bulk = (char)RedisMessage.Bulk;
        const char MultiBulk = (char)RedisMessage.MultiBulk;

        readonly byte[][] _chunks;
        readonly Encoding _encoding;
        readonly byte[] _bulkBytes;
        readonly byte[] _multiBulkBytes;
        readonly byte[] _eolBytes;
        int _size;
        int _chunkPosition;


        public RedisWriter(int size)
            : this(size, Encoding.UTF8) { }

        public RedisWriter(int size, Encoding encoding)
        {
            _chunks = new byte[size][];
            _encoding = encoding;
            _bulkBytes = _encoding.GetBytes(new[] { Bulk });
            _multiBulkBytes = _encoding.GetBytes(new[] { MultiBulk });
            _eolBytes = _encoding.GetBytes(RedisConnection.EOL);
        }

        public void WriteBulk(object data)
        {
            if (data is byte[])
                WriteBulk(data as byte[]);
            else if (data is String)
                WriteBulk(data as String);
            else
                WriteBulk(data.ToString());
        }

        public void WriteBulk(string data)
        {
            WriteBulk(_encoding.GetBytes(data));
        }

        public void WriteBulk(byte[] data)
        {
            byte[] len = _encoding.GetBytes(data.Length.ToString());
            byte[] output = new byte[_bulkBytes.Length + len.Length + _eolBytes.Length + data.Length + _eolBytes.Length];

            int p = 0;

            Buffer.BlockCopy(_bulkBytes, 0, output, p, _bulkBytes.Length);
            p += _bulkBytes.Length;

            Buffer.BlockCopy(len, 0, output, p, len.Length);
            p += len.Length;

            Buffer.BlockCopy(_eolBytes, 0, output, p, _eolBytes.Length);
            p += _eolBytes.Length;

            Buffer.BlockCopy(data, 0, output, p, data.Length);
            p += data.Length;

            Buffer.BlockCopy(_eolBytes, 0, output, p, _eolBytes.Length);

            _chunks[_chunkPosition++] = output;
            _size += output.Length;
        }

        public byte[] ToArray()
        {
            byte[] len = _encoding.GetBytes(_chunks.Length.ToString());
            byte[] output = new byte[_multiBulkBytes.Length + len.Length + _eolBytes.Length + _size];

            int p = 0;

            Buffer.BlockCopy(_multiBulkBytes, 0, output, p, _multiBulkBytes.Length);
            p += _multiBulkBytes.Length;

            Buffer.BlockCopy(len, 0, output, p, len.Length);
            p += len.Length;

            Buffer.BlockCopy(_eolBytes, 0, output, p, _eolBytes.Length);
            p += _eolBytes.Length;


            for (int i = 0; i < _chunks.Length; i++)
            {
                Buffer.BlockCopy(_chunks[i], 0, output, p, _chunks[i].Length);
                p += _chunks[i].Length;
            }

            return output;
        }

        public override string ToString()
        {
            return _encoding.GetString(ToArray());
        }
    }
}
