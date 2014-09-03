using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace CSRedis.Internal.IO
{
    class RedisSocket : IRedisSocket
    {
        Socket _socket;

        public bool Connected { get { return _socket == null ? false : _socket.Connected; } }

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

        public RedisSocket()
        { }

        public void Connect(EndPoint endpoint)
        {
            InitSocket();
            _socket.Connect(endpoint);
        }

        public bool ConnectAsync(SocketAsyncEventArgs args)
        {
            InitSocket();
            return _socket.ConnectAsync(args);
        }

        public bool SendAsync(SocketAsyncEventArgs args)
        {
            return _socket.SendAsync(args);
        }

        public Stream GetStream()
        {
            return new NetworkStream(_socket);
        }

        public void Dispose()
        {
            _socket.Dispose();
        }

        void InitSocket()
        {
            if (_socket != null)
                _socket.Dispose();

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }
    }
}
