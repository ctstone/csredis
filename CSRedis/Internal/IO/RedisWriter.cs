using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    class RedisWriter : IDisposable
    {
        const char Bulk = (char)RedisMessage.Bulk;
        const char MultiBulk = (char)RedisMessage.MultiBulk;

        readonly object _asyncWriteLock = new object();
        readonly Stream _streamBuffer;
        readonly Encoding _encoding;
        readonly static byte[] _bulkPrefix = Encoding.ASCII.GetBytes(new[] { Bulk });
        readonly static byte[] _multiBulkPrefix = Encoding.ASCII.GetBytes(new[] { MultiBulk });
        readonly static byte[] _eolBytes = Encoding.ASCII.GetBytes(RedisConnection.EOL);

        public RedisWriter(Stream stream, Encoding encoding)
        {
            _streamBuffer = stream;
            _encoding = encoding;
        }

        public void Write(string command, params object[] args)
        {
            string[] parts = command.Split(' ');
            WriteMultiBulk(parts.Length + args.Length);

            foreach (var part in parts)
                WriteBulk(part);

            foreach (var arg in args)
                WriteBulk(arg);

            _streamBuffer.Flush();
        }
        public Task WriteAsync(string command, params object[] args)
        {
            lock (_asyncWriteLock)
            {
                string[] parts = command.Split(' ');
                return WriteMultiBulkAsync(parts.Length + args.Length)
                    .ContinueWith(t1 => Walk(parts, 0, x => WriteBulkAsync(x))
                        .ContinueWith(t2 => Walk(args, 0, x => WriteBulkAsync(x))
                            .ContinueWith(t3 =>
                            {
                                _streamBuffer.Flush();
                                return t3;
                            })))
                        .Result
                    .Result
                .Result;
            }
        }

        Task Walk(object[] parts, int i, Func<object, Task> action)
        {
            if (i >= parts.Length)
            {
                var tcs = new TaskCompletionSource<bool>();
                tcs.SetResult(true);
                return tcs.Task;
            }

            return action(parts[i++])
                .ContinueWith<Task>(t2 => Walk(parts, i, action))
            .Result;
        }


        void WriteMultiBulk(int size)
        {
            byte[] length = _encoding.GetBytes(size.ToString());
            Write(_multiBulkPrefix);
            Write(length);
            WriteEOL();
        }
        Task WriteMultiBulkAsync(int size)
        {
            byte[] length = _encoding.GetBytes(size.ToString());
            return WriteAsync(_multiBulkPrefix)
                .ContinueWith(t1 => WriteAsync(length)
                    .ContinueWith(t2 => WriteEOLAsync()))
                .Result
            .Result;
        }


        void WriteEOL()
        {
            Write(_eolBytes);
        }
        Task WriteEOLAsync()
        {
            return WriteAsync(_eolBytes);
        }


        void WriteBulk(object data)
        {
            if (data is byte[])
                WriteBulk(data as byte[]);
            else if (data is String)
                WriteBulk(data as String);
            else if (data is double)
                WriteBulk(((double) data).ToString(CultureInfo.InvariantCulture));
            else if (data is float)
                WriteBulk(((float)data).ToString(CultureInfo.InvariantCulture));
            else
                WriteBulk(data.ToString());
        }
        Task WriteBulkAsync(object data)
        {
            if (data is byte[])
                return WriteBulkAsync(data as byte[]);
            else if (data is String)
                return WriteBulkAsync(data as String);
            else
                return WriteBulkAsync(data.ToString());
        }


        void WriteBulk(string data)
        {
            WriteBulk(_encoding.GetBytes(data));
        }
        Task WriteBulkAsync(string data)
        {
            return WriteBulkAsync(_encoding.GetBytes(data));
        }


        void WriteBulk(byte[] data)
        {
            byte[] len = _encoding.GetBytes(data.Length.ToString());
            Write(_bulkPrefix);
            Write(len);
            WriteEOL();
            Write(data);
            WriteEOL();
        }
        Task WriteBulkAsync(byte[] data)
        {
            byte[] len = _encoding.GetBytes(data.Length.ToString());

            return WriteAsync(_bulkPrefix)
                .ContinueWith(t1 => WriteAsync(len)
                    .ContinueWith(t2 => WriteEOLAsync()
                        .ContinueWith(t3 => WriteAsync(data)
                            .ContinueWith(t4 => WriteEOLAsync()))))
                        .Result
                    .Result
                .Result
            .Result;
        }

        void Write(byte[] data)
        {
            _streamBuffer.Write(data, 0, data.Length);
        }

        Task WriteAsync(byte[] data)
        {
            var tcs = new TaskCompletionSource<bool>();
            _streamBuffer.BeginWrite(data, 0, data.Length, ar =>
            {
                _streamBuffer.EndWrite(ar);
                tcs.SetResult(true);
            }, null);
            return tcs.Task;
        }

        public void Dispose()
        {
            if (_streamBuffer.CanWrite)
                _streamBuffer.Flush();
            _streamBuffer.Dispose();
        }
    }
}
