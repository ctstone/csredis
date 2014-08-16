using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.Old
{
    interface IRedisConnector : IDisposable
    {
        bool Connected { get; }
        string Host { get; }
        int Port { get; }
        int ReceiveTimeout { get; set; }
        int SendTimeout { get; set; }

        Stream Connect(int timeout);
        Stream Reconnect(int timeout);
        Task<Stream> ConnectAsync();
        Task<Stream> ReconnectAsync();
        void OnWriteFlushed();
    }

    interface IRedisConnector2 : IDisposable
    {
        bool Connected { get; }
        string Host { get; }
        int Port { get; }
        int ReceiveTimeout { get; set; }
        int SendTimeout { get; set; }

        bool Connect(int timeout);
        bool Reconnect(int timeout);
        void Write(byte[] buffer, int count, int offset);
        int Read(byte[] buffer, int count, int offset);
        
        Task<bool> ConnectAsync();
        Task<bool> ReconnectAsync();
        Task WriteAsync(byte[] buffer, int count, int offset);
        Task<int> ReadAsync(byte[] buffer, int count, int offset);
    }

    class DefaultConnector2 : IRedisConnector2
    {
        readonly string _host;
        readonly int _port;
        readonly object _connectionLock = new object();
        TaskCompletionSource<bool> _connectionSource;
        bool _connectionStarted;
        Socket _socket;

        public string Host { get { return _host; } }
        public int Port { get { return _port; } }
        public bool Connected { get { return _socket.Connected; } }
        public int ReceiveTimeout
        {
            get { return _socket.ReceiveTimeout; }
            set { _socket.ReceiveTimeout = value; }
        }
        public int SendTimeout
        {
            get { return _socket.SendTimeout; }
            set { _socket.SendTimeout = value; }
        }

        public DefaultConnector2(string host, int port)
        {
            _host = host;
            _port = port;
        }


        public bool Connect(int timeout)
        {
            lock (_connectionLock)
            {
                if (_connectionStarted)
                    return _connectionSource.Task.Result;
                _connectionStarted = true;
            }

            if (timeout > 0)
                _socket.BeginConnect(_host, _port, null, null).AsyncWaitHandle.WaitOne(timeout, true);
            else
                _socket.Connect(_host, _port);

            if (_socket.Connected)
            {
                Stream stream = new NetworkStream(_socket);
                _connectionSource.SetResult(true);
                return true;
            }

            return false;
        }

        public Task<bool> ConnectAsync()
        {
            lock (_connectionLock)
            {
                if (_connectionStarted)
                    return _connectionSource.Task;
                _connectionStarted = true;
            }

            var args = new SocketAsyncEventArgs
            {
                RemoteEndPoint = new DnsEndPoint(Host, Port),
            };
            args.Completed += (x, y) => _connectionSource.SetResult(true);
            if (!_socket.ConnectAsync(args))
                _connectionSource.SetResult(true);

            return _connectionSource.Task;
        }

        public bool Reconnect(int timeout)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ReconnectAsync()
        {
            throw new NotImplementedException();
        }

        public void Write(byte[] buffer, int count, int offset)
        {
            _socket.Send(buffer, offset, count, SocketFlags.None);
        }

        public Task WriteAsync(byte[] buffer, int count, int offset)
        {
            var tcs = new TaskCompletionSource<bool>();
            var args = new SocketAsyncEventArgs();
            args.Completed += (x, y) => tcs.SetResult(true);
            args.SetBuffer(buffer, count, offset);
            if (!_socket.SendAsync(args))
                tcs.SetResult(true);

            return tcs.Task;
        }

        public int Read(byte[] buffer, int count, int offset)
        {
            return _socket.Receive(buffer, offset, count, SocketFlags.None);
        }

        public Task<int> ReadAsync(byte[] buffer, int count, int offset)
        {
            var tcs = new TaskCompletionSource<int>();
            var args = new SocketAsyncEventArgs();

            args.SetBuffer(buffer, offset, count);
            args.Completed += (x, y) => tcs.SetResult(args.BytesTransferred);
            if (!_socket.ReceiveAsync(args))
                tcs.SetResult(args.BytesTransferred);

            return tcs.Task;
        }

        public void Dispose()
        {
            _socket.Dispose();
        }
    }
}
