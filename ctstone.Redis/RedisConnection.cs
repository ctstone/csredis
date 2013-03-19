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
        public bool Connect(int millisecondsTimeout)
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
                _stream = new NetworkStream(_socket);
            else
                _socket.Close();

            return _socket.Connected;
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
        /// Read next strongly-typed object form the Redis server
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
                _asyncReader.Wait();
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
                parserTask.Start();
                parserTask.Wait();
            }
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
