using CSRedis.Internal.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    class MockConnector : IRedisConnector
    {
        readonly string _host;
        readonly int _port;
        readonly Stream _written;
        readonly Stream _responses;
        readonly RedisWriter _writer;
        readonly RedisReader _reader;
        readonly RedisEncoding _encoding;
        readonly ConcurrentQueue<Tuple<long, int>> _messages;
        readonly RedisPipeline _pipeline;
        bool _connected;
        
        public event EventHandler Reconnected;

        public bool Connected { get { return _connected; } }
        public string Host { get { return _host; } }
        public int Port { get { return _port; } }
        public int ReceiveTimeout { get; set; }
        public int SendTimeout { get; set; }
        public int ReconnectAttempts { get; set; }
        public int ReconnectWait { get; set; }
        public bool Pipelined { get { return _pipeline.Active; } }

        public Encoding Encoding
        {
            get { return _encoding.Encoding; }
            set { _encoding.Encoding = value; }
        }

        public MockConnector(string host, int port, params string[] mockResponses)
            : this(host, port, new UTF8Encoding(false), mockResponses)
        { }

        public MockConnector(string host, int port, Encoding encoding, params string[] mockResponses)
            : this(host, port, ToBytes(mockResponses, encoding))
        {
            Encoding = encoding;
        }

        public MockConnector(string host, int port, params byte[][] mockResponses)
        {
            _host = host;
            _port = port;
            _encoding = new RedisEncoding();
            _written = new MemoryStream();
            _responses = new MemoryStream();
            _writer = new RedisWriter(_encoding);
            _reader = new RedisReader(_encoding, _responses);
            _messages = new ConcurrentQueue<Tuple<long, int>>();
            _pipeline = new RedisPipeline(_written, _encoding, _reader);

            for (int i = 0; i < mockResponses.Length; i++)
                _responses.Write(mockResponses[i], 0, mockResponses[i].Length);

            _responses.Position = 0;
        }

        public bool Connect()
        {
            return _connected = true;
        }

        public Task<bool> ConnectAsync()
        {
            var tcs = new TaskCompletionSource<bool>();
            tcs.SetResult(_connected = true);
            return tcs.Task;
        }

        public T Call<T>(RedisCommand<T> command)
        {
            Write(command);
            return Read(command.Parse);
        }

        public Task<T> CallAsync<T>(RedisCommand<T> command)
        {
            var tcs = new TaskCompletionSource<T>();
            lock (_writer)
            {
                TrySetResult(tcs, command);
            }
            return tcs.Task;
        }

        

        public void Write(RedisCommand command)
        {
            _messages.Enqueue(Tuple.Create(_written.Position, _writer.Write(command, _written)));
        }

        public T Read<T>(Func<IO.RedisReader, T> func)
        {
            return func(_reader);
        }

        public void Read(Stream destination, int bufferSize)
        {
            throw new NotImplementedException();
        }

        public string GetMessage()
        {
            Tuple<long, int> info;
            if (!_messages.TryDequeue(out info))
                throw new Exception();

            long position = _written.Position;
            byte[] buffer = new byte[info.Item2];
            _written.Position = info.Item1;
            _written.Read(buffer, 0, buffer.Length);
            _written.Position = position;
            return Encoding.GetString(buffer, 0, buffer.Length);
        }

        public void BeginPipe()
        {
            _pipeline.Begin();
        }

        public object[] EndPipe()
        {
            return _pipeline.Flush();
        }

        public void Dispose()
        {
            //throw new NotImplementedException(); // TODO
        }

        static byte[][] ToBytes(string[] strings, Encoding encoding)
        {
            byte[][] data = new byte[strings.Length][];
            for (int i = 0; i < data.Length; i++)
                data[i] = encoding.GetBytes(strings[i]);
            return data;
        }

        void TrySetResult<T>(TaskCompletionSource<T> tcs, RedisCommand<T> command)
        {
            try
            {
                tcs.SetResult(Call(command));
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }
        }
    }
}
