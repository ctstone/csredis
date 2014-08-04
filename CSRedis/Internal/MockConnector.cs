using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    class BlockingStream : Stream
    {
        readonly Stream _stream;
        readonly MockConnector _connector;


        public BlockingStream(Stream stream, MockConnector connector)
        {
            if (!stream.CanSeek)
                throw new ArgumentException("Stream must support seek", "stream");
            _stream = stream;
            _connector = connector;
        }

        public override void Flush()
        {
            lock (_stream)
            {
                _stream.Flush();
                Monitor.Pulse(_stream);
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            lock (_stream)
            {
                long res = _stream.Seek(offset, origin);
                Monitor.Pulse(_stream);
                return res;
            }
        }

        public override void SetLength(long value)
        {
            lock (_stream)
            {
                _stream.SetLength(value);
                Monitor.Pulse(_stream);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            lock (_stream)
            {
                do
                {
                    int read = _stream.Read(buffer, offset, count);
                    if (read > 0)
                        return read;
                    bool timed_out = !Monitor.Wait(_stream, 100); // TODO expose Timeout parameter
                    if (timed_out)
                    {
                        _connector.Close(); // TODO is this really necessary?
                        throw new IOException("Timed out", new TimeoutException());
                    }
                } while (true);
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (_stream)
            {
                _stream.Write(buffer, offset, count);
                Monitor.Pulse(_stream);
            }
        }

        public override bool CanRead
        {
            get
            {
                lock (_stream)
                {
                    return _stream.CanRead;
                }
            }
        }

        public override bool CanSeek
        {
            get
            {
                lock (_stream)
                {
                    return _stream.CanSeek;
                }
            }
        }

        public override bool CanWrite
        {
            get
            {
                lock (_stream)
                {
                    return _stream.CanWrite;
                }
            }
        }

        public override long Length
        {
            get
            {
                lock (_stream)
                {
                    return _stream.Length;
                }
            }
        }

        public override long Position
        {
            get
            {
                lock (_stream)
                {
                    return _stream.Position;
                }
            }
            set
            {
                lock (_stream)
                {
                    _stream.Position = value;
                    Monitor.Pulse(_stream);
                }
            }
        }
    }

    class MockConnector : IRedisConnector
    {
        readonly Stream _stream;
        readonly Encoding _encoding;
        readonly string _host;
        readonly int _port;
        readonly byte[][] _responses;
        readonly byte[][] _messages;
        int _index;
        int _messageIndex;
        long _position;
        bool _connected;

        public MockConnector(string host, int port, params string[] mockResponses)
            : this(host, port, new UTF8Encoding(false), false, mockResponses)
        { }
        public MockConnector(string host, int port, bool blocking, params string[] mockResponses)
            : this(host, port, new UTF8Encoding(false), blocking, mockResponses)
        { }
        public MockConnector(string host, int port, Encoding encoding, params string[] mockResponses)
            : this(host, port, false, ToBytes(mockResponses, encoding))
        {
            _encoding = encoding;
        }
        public MockConnector(string host, int port, Encoding encoding, bool blocking, params string[] mockResponses)
            : this(host, port, blocking, ToBytes(mockResponses, encoding))
        {
            _encoding = encoding;
        }
        public MockConnector(string host, int port, bool blocking, params byte[][] mockResponses)
        {
            _host = host;
            _port = port;
            _responses = mockResponses;
            _messages = new byte[_responses.Length][];

            if (blocking)
                _stream = new BlockingStream(new MemoryStream(), this);
            else
                _stream = new MemoryStream();
        }

        public bool Connected { get { return _connected; } }
        public string Host { get { return _host; } }
        public int Port { get { return _port; } }
        public int ReceiveTimeout { get; set; }
        public int SendTimeout { get; set; }
        public Stream Connect(int timeout)
        {
            _connected = true;
            return _stream;
        }

        public Task<Stream> ConnectAsync()
        {
            var tcs = new TaskCompletionSource<Stream>();
            tcs.SetResult(_stream);
            _connected = true;
            return tcs.Task;
        }

        public void Close()
        {
            _connected = false;
        }

        public void Dispose()
        {
            _connected = false;
            _stream.Dispose();
        }

        public string GetMessage()
        {
            if (_messageIndex >= _messages.Length)
                throw new InvalidOperationException("No more messages");
            return _encoding.GetString(_messages[_messageIndex++]);
        }

        public void OnWriteFlushed()
        {
            lock (_stream)
            {
                if (_index >= _messages.Length)
                    throw new InvalidOperationException("Unexpected write. Add a mock reply?");

                byte[] message = new byte[_stream.Position - _position];
                _stream.Position = _position;
                _stream.Read(message, 0, message.Length);
                _messages[_index] = message;

                byte[] response = _responses[_index++];
                _stream.Write(response, 0, response.Length);
                _position = _stream.Position;
                _stream.Position -= response.Length;
            }
        }

        static byte[][] ToBytes(string[] strings, Encoding encoding)
        {
            byte[][] data = new byte[strings.Length][];
            for (int i = 0; i < data.Length; i++)
                data[i] = encoding.GetBytes(strings[i]);
            return data;
        }


        public Stream Reconnect(int timeout)
        {
            throw new NotImplementedException();
        }


        public Task<Stream> ReconnectAsync()
        {
            throw new NotImplementedException();
        }
    }
}
