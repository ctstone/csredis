using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    class DefaultConnector : IRedisConnector
    {
        readonly string _host;
        readonly int _port;
        readonly object _connectionLock = new object();
        TaskCompletionSource<Stream> _connectionSource;
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

        public DefaultConnector(string host, int port)
        {
            _host = host;
            _port = port;
            ResetConnection();
        }

        public Stream Reconnect(int timeout)
        {
            ResetConnection();
            return Connect(timeout);
        }

        public Task<Stream> ReconnectAsync()
        {
            ResetConnection();
            return ConnectAsync();
        }


        public Stream Connect(int timeout)
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
                _connectionSource.SetResult(stream);
                return stream;
            }

            return null;
        }

        public Task<Stream> ConnectAsync()
        {
            lock (_connectionLock)
            {
                if (_connectionStarted)
                    return _connectionSource.Task;
                _connectionStarted = true;
            }

            _socket.BeginConnect(_host, _port, ar =>
            {
                _socket.EndConnect(ar);
                _connectionSource.SetResult(new NetworkStream(_socket));
            }, null);

            return _connectionSource.Task;
        }

        public void Dispose()
        {
            _socket.Dispose();
        }


        public void OnWriteFlushed()
        { }

        void ResetConnection()
        {
            if (_socket != null)
            {
                _socket.Dispose();
                _socket = null;
            }

            _connectionSource = new TaskCompletionSource<Stream>();
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _connectionStarted = false;
        }
    }
}
