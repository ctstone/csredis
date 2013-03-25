using System;
using System.Collections.Concurrent;
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
        private static Encoding _encoding = Encoding.UTF8;
        private Socket _socket;
        private Stream _stream;
        private BlockingCollection<Task> _asyncTaskQueue;
        private Task _asyncReader;
        private readonly object _asyncLock;
        private long _bytesRemaining;
        
        /// <summary>
        /// Instantiate new instance of RedisConnection
        /// </summary>
        /// <param name="host">Redis server hostname or IP</param>
        /// <param name="port">Redis server port</param>
        public RedisConnection(string host, int port)
        {
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
        /// <returns>True if connected</returns>
        public bool Connect(int millisecondsTimeout, int readTimeout = 0)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            
            if (millisecondsTimeout > 0)
            {
                IAsyncResult ar = _socket.BeginConnect(Host, Port, null, null);
                ar.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
            }
            else
            {
                _socket.Connect(Host, Port);
            }
            
            if (_socket.Connected)
            {
                _stream = new NetworkStream(_socket);
                if (readTimeout > 0)
                    _stream.ReadTimeout = readTimeout;
            }
            else
            {
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
            RedisMessage type = RedisReader.ReadType(_stream);

            if (type == RedisMessage.Error)
                throw new RedisException(RedisReader.ReadStatus(_stream, false));

            if (type != RedisMessage.Bulk)
                throw new InvalidOperationException("Cannot stream from non-bulk response. Received: " + type);

            RedisReader.ReadBulk(_stream, destination, bufferSize, false);
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
            if (offset > buffer.Length || count > buffer.Length)
                throw new InvalidOperationException("Buffer offset or count is larger than buffer");

            if (!Buffering)
            {
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
        /// Write command to Redis server
        /// </summary>
        /// <param name="command">Base Redis command</param>
        /// <param name="arguments">Array of command arguments</param>
        public void Write(string command, params object[] arguments)
        {
            byte[] buffer = _encoding.GetBytes(CreateMessage(command, arguments));
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
            byte[] buffer = _encoding.GetBytes(CreateMessage(command, arguments));
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
            byte[] buffer = _encoding.GetBytes(CreateMessage(command, arguments));
            Task<T> task = new Task<T>(() => parser(_stream));
            //Console.WriteLine("Created task {0}", task.Id);

            lock (_asyncLock)
            {
                //Console.WriteLine("Begin lock: task {0}", task.Id);
                //_stream.WriteAsync(buffer, 0, buffer.Length); // .NET 4.5
                _stream.BeginWrite(buffer, 0, buffer.Length, null, null);
                _asyncTaskQueue.Add(task);
                _ms.Write(buffer, 0, buffer.Length);
                _ms.Write(Encoding.UTF8.GetBytes("|"), 0, 1);
                //Console.WriteLine("End lock: task {0}", task.Id);
            }
            return task;
        }
        private MemoryStream _ms = new MemoryStream();
        public string Temp { get { return Encoding.UTF8.GetString(_ms.ToArray()); } }

        /// <summary>
        /// Asyncronously write command to Redis request buffer
        /// </summary>
        /// <param name="command">Base Redis base command</param>
        /// <param name="arguments">Array of command arguments</param>
        public void WriteAsync(string command, params object[] arguments)
        {
            byte[] buffer = _encoding.GetBytes(CreateMessage(command, arguments));
            //return _stream.WriteAsync(buffer, 0, buffer.Length); // .NET 4.5
            _stream.BeginWrite(buffer, 0, buffer.Length, null, null);
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

            if (_stream != null)
                _stream.Dispose();

            if (_socket != null)
                _socket.Dispose();
        }

        private void Read_Task()
        {
            foreach (var parserTask in _asyncTaskQueue.GetConsumingEnumerable())
            {
                Console.WriteLine("Running task {0} ({1})", parserTask.Id, _asyncTaskQueue.Count);
                parserTask.RunSynchronously();
                if (parserTask.Exception != null)
                {
                    bool is_fatal = !CanHandleException(parserTask.Exception);
                    if (TaskReadExceptionOccurred != null)
                        TaskReadExceptionOccurred(parserTask, new UnhandledExceptionEventArgs(parserTask.Exception, is_fatal));
                    if (is_fatal)
                        throw parserTask.Exception;
                }
                Console.WriteLine("Completed task {0} ({1})", parserTask.Id, _asyncTaskQueue.Count);
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

        private static string CreateMessage(string command, params object[] args)
        {
            string[] cmd = RedisArgs.Concat(command.ToString().Split(' '), args);

            StringBuilder cmd_builder = new StringBuilder();

            cmd_builder
                .Append(MultiBulk)
                .Append(cmd.Length)
                .Append(EOL);

            foreach (var arg in cmd)
            {
                cmd_builder
                    .Append(Bulk)
                    .Append(arg.Length)
                    .Append(EOL);
                cmd_builder
                    .Append(arg)
                    .Append(EOL);
            }

            return cmd_builder.ToString();
        }
    }
}
