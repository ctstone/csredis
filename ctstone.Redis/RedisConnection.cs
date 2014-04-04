using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    /// <summary>
    /// Provides network connection to a Redis server
    /// </summary>
    class RedisConnection : IDisposable
    {
        /// <summary>
        /// End-of-line string used by Redis server
        /// </summary>
        public const string EOL = "\r\n";

        /// <summary>
        /// Redis server hostname
        /// </summary>
        public string Host { get; private set; }

        /// <summary>
        /// Redis server port
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Get a value indicating that the Redis server connection is open
        /// </summary>
        public bool Connected { get { return _socket.Connected; } }

        /// <summary>
        /// Get or set the value indicating that the current connection is in read-buffering mode
        /// </summary>
        public bool Buffering { get; set; }


        /// <summary>
        /// Occurs when a background task raises an exception
        /// </summary>
        public event UnhandledExceptionEventHandler TaskReadExceptionOccurred;

        private const char Error = (char)RedisMessage.Error;
        private const char Status = (char)RedisMessage.Status;
        private const char Bulk = (char)RedisMessage.Bulk;
        private const char MultiBulk = (char)RedisMessage.MultiBulk;
        private const char Int = (char)RedisMessage.Int;
        private static Encoding _encoding = new UTF8Encoding(false);
        private Socket _socket;
        private Stream _stream;
        private BlockingCollection<Task> _asyncTaskQueue;
        private Task _asyncReader;
        private readonly object _asyncLock;
        private long _bytesRemaining;
        private ActivityTracer _activity;
        
        /// <summary>
        /// Instantiate new instance of RedisConnection
        /// </summary>
        /// <param name="host">Redis server hostname or IP</param>
        /// <param name="port">Redis server port</param>
        public RedisConnection(string host, int port)
        {
            _activity = new ActivityTracer("New Redis connection");
            Host = host;
            Port = port;
            _asyncLock = new object();
            _asyncTaskQueue = new BlockingCollection<Task>();
            _asyncReader = Task.Factory.StartNew(Read_Task); // TODO: cancel token
        }

        /// <summary>
        /// Open connection to the Redis server
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout to wait for connection (0 for no timeout)</param>
        /// <param name="readTimeout">Time to wait for reading (0 for no timeout)</param>
        /// <returns>True if connected</returns>
        public bool Connect(int millisecondsTimeout, int readTimeout = 0)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            ActivityTracer.Verbose("Opening connection with {0} ms timeout", millisecondsTimeout);
            if (millisecondsTimeout > 0)
                _socket
                    .BeginConnect(Host, Port, null, null)
                    .AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
            else
                _socket.Connect(Host, Port);

            if (_socket.Connected)
            {
                ActivityTracer.Info("Connected. Read timeout is {0}", readTimeout);
                _stream = new NetworkStream(_socket);
                if (readTimeout > 0)
                    _stream.ReadTimeout = readTimeout;
            }
            else
            {
                ActivityTracer.Info("Connection timed out");
                _socket.Close();
            }

            return _socket.Connected;
        }

        /// <summary>
        /// Read response from server into a stream
        /// </summary>
        /// <param name="destination">The stream that will contain the contents of the server response.</param>
        /// <param name="bufferSize">Size of internal buffer used to copy streams</param>
        public void Read(Stream destination, int bufferSize)
        {
            using (new ActivityTracer("Read response to stream"))
            {
                ActivityTracer.Verbose("Buffer size is {0}", bufferSize);
                RedisMessage type = RedisReader.ReadType(_stream);

                if (type == RedisMessage.Error)
                    throw new RedisException(RedisReader.ReadStatus(_stream, false));

                if (type != RedisMessage.Bulk)
                    throw new InvalidOperationException("Cannot stream from non-bulk response. Received: " + type);

                RedisReader.ReadBulk(_stream, destination, bufferSize, false);
            }
        }

        /// <summary>
        /// Read server response bytes into buffer and advance the server response stream (requires Buffering=true)
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>
        public int Read(byte[] buffer, int offset, int count)
        {
            using (new ActivityTracer("Read response to buffer"))
            {
                ActivityTracer.Verbose("Offset={0}; Count={1}", offset, count);
                if (offset > buffer.Length || count > buffer.Length)
                    throw new InvalidOperationException("Buffer offset or count is larger than buffer");

                if (!Buffering)
                {
                    ActivityTracer.Verbose("Not buffering; zeroing out buffer");
                    for (int i = offset; i < count; i++)
                        buffer[i] = 0;
                    return 0;
                }

                if (_bytesRemaining == 0)
                {
                    RedisMessage type = RedisReader.ReadType(_stream);

                    if (type == RedisMessage.Error)
                        throw new RedisException(RedisReader.ReadStatus(_stream, false));

                    if (type != RedisMessage.Bulk)
                        throw new InvalidOperationException("Cannot buffer from non-bulk response. Received: " + type);

                    _bytesRemaining = RedisReader.ReadInt(_stream, false);
                }

                ActivityTracer.Verbose("Bytes remaining: {0}", _bytesRemaining);

                int bytes_to_read = count;
                if (bytes_to_read > _bytesRemaining)
                    bytes_to_read = (int)_bytesRemaining;

                int bytes_read = 0;
                while (bytes_read < bytes_to_read)
                    bytes_read += _stream.Read(buffer, offset + bytes_read, bytes_to_read - bytes_read);

                _bytesRemaining -= bytes_read;

                if (_bytesRemaining == 0)
                {
                    RedisReader.ReadCRLF(_stream);
                    Buffering = false;
                }

                return bytes_read;
            }
        }

        /// <summary>
        /// Read next object from Redis response
        /// </summary>
        /// <returns>Next object in response buffer</returns>
        public object Read()
        {
            return RedisReader.Read(_stream);
        }

        /// <summary>
        /// Read next strongly-typed object from the Redis server
        /// </summary>
        /// <typeparam name="T">Type of object that will be read</typeparam>
        /// <param name="parser">Redis parser method</param>
        /// <returns>Next object in response buffer</returns>
        public T Read<T>(Func<Stream, T> parser)
        {
            return parser(_stream);
        }

        /// <summary>
        /// Read next strongly-typed object from the Redis server
        /// </summary>
        /// <param name="parser">A delegate method accepting a Stream and returning a parsed object</param>
        /// <returns>Parsed object from delegate</returns>
        public object Read(Delegate parser)
        {
            return parser.DynamicInvoke(_stream);
        }

        /// <summary>
        /// Write command to Redis server
        /// </summary>
        /// <param name="command">Base Redis command</param>
        /// <param name="arguments">Array of command arguments</param>
        public void Write(string command, params object[] arguments)
        {
            byte[] buffer = CreateMessage(command, arguments);
            _stream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Write command to Redis server and return strongly-typed result
        /// </summary>
        /// <typeparam name="T">Type of object that will be read</typeparam>
        /// <param name="parser">Redis parser method</param>
        /// <param name="command">Base Redis command</param>
        /// <param name="arguments">Array of command arguments</param>
        /// <returns>Command response</returns>
        public T Call<T>(Func<Stream, T> parser, string command, params object[] arguments)
        {
            byte[] buffer = CreateMessage(command, arguments);
            _stream.Write(buffer, 0, buffer.Length);
            return parser(_stream);
        }

        /// <summary>
        /// Asyncronously write command to Redis server
        /// </summary>
        /// <typeparam name="T">Type of object that will be read</typeparam>
        /// <param name="parser">Redis parser method</param>
        /// <param name="command">Base Redis command</param>
        /// <param name="arguments">Array of command arguments</param>
        /// <returns>Task that will return strongly-typed Redis response when complete</returns>
        public Task<T> CallAsync<T>(Func<Stream, T> parser, string command, params object[] arguments) 
        {
            byte[] buffer = CreateMessage(command, arguments);
            Task<T> task = new Task<T>(() => parser(_stream));

            lock (_asyncLock)
            {
                //_stream.WriteAsync(buffer, 0, buffer.Length); // .NET 4.5
                _stream.BeginWrite(buffer, 0, buffer.Length, null, null);
                _asyncTaskQueue.Add(task);
            }
            return task;
        }

        /// <summary>
        /// Asyncronously write command to Redis request buffer
        /// </summary>
        /// <param name="command">Base Redis base command</param>
        /// <param name="arguments">Array of command arguments</param>
        public Task WriteAsync(string command, params object[] arguments)
        {
            //byte[] buffer = _encoding.GetBytes(CreateMessage(command, arguments));
            //return _stream.WriteAsync(buffer, 0, buffer.Length); // .NET 4.5
            byte[] buffer = CreateMessage(command, arguments);
            return Task.Factory.StartNew(() => _stream.Write(buffer, 0, buffer.Length));
        }

        /// <summary>
        /// Release resources used by the current RedisConnection
        /// </summary>
        public void Dispose()
        {
            if (_asyncTaskQueue != null)
                _asyncTaskQueue.CompleteAdding();

            if (_asyncReader != null)
            {
                try
                {
                    _asyncReader.Wait();
                }
                catch (AggregateException ae)
                {
                    throw ae;
                }
                _asyncReader.Dispose();
                _asyncReader = null;
            }

            if (_asyncTaskQueue != null)
            {
                _asyncTaskQueue.Dispose();
                _asyncTaskQueue = null;
            }

            ActivityTracer.Verbose("Closing connection stream");
            if (_stream != null)
                _stream.Dispose();

            ActivityTracer.Info("Closing connection");
            if (_socket != null)
                _socket.Dispose();

            if (_activity != null)
                _activity.Dispose();
            _activity = null;
        }

        private void Read_Task()
        {
            using (new ActivityTracer("Read async"))
            {
                foreach (var parserTask in _asyncTaskQueue.GetConsumingEnumerable())
                {
                    parserTask.RunSynchronously();
                    if (parserTask.Exception != null)
                    {
                        bool is_fatal = !CanHandleException(parserTask.Exception);
                        if (TaskReadExceptionOccurred != null)
                            TaskReadExceptionOccurred(parserTask, new UnhandledExceptionEventArgs(parserTask.Exception, is_fatal));
                        if (is_fatal)
                        {
                            ActivityTracer.Error(parserTask.Exception);
                            throw parserTask.Exception;
                        }
                        else
                        {
                            ActivityTracer.Warn(parserTask.Exception);
                        }
                    }
                }
            }
        }

        private bool CanHandleException(AggregateException ex)
        {
            foreach (var inner in ex.InnerExceptions)
            {
                if (!(inner is RedisProtocolException || inner is RedisException))
                    return false;
            }
            return true;
        }

        byte[] CreateMessage(string command, params object[] args)
        {
            using (new ActivityTracer("Create message"))
            {
                ActivityTracer.Source.TraceEvent(TraceEventType.Information, 0, "Command: {0}", command);

                string[] parts = command.Split(' ');
                int len = parts.Length + args.Length;

                RedisWriter writer = new RedisWriter(len, _encoding);
                foreach (var part in parts)
                    writer.WriteBulk(part);

                foreach (var arg in args)
                    writer.WriteBulk(arg);

                return writer.ToArray();
            }
        }
    }
}
