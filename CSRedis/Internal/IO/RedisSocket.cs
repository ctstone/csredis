using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    class RedisSocket : IRedisSocket
    {
        readonly bool _ssl;
        readonly EndPoint _endPoint;
        Socket _socket;

        public bool SSL { get { return _ssl; } }
        public EndPoint EndPoint { get { return _endPoint; } }

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

        public RedisSocket(EndPoint endPoint, bool ssl)
        {
            _ssl = ssl;
            _endPoint = endPoint;
        }

        public void Connect()
        {
            InitSocket();
            _socket.Connect(_endPoint);
        }

        public bool ConnectAsync(SocketAsyncEventArgs args)
        {
            args.RemoteEndPoint = _endPoint;
            InitSocket();
            return _socket.ConnectAsync(args);
        }

        public Task<bool> ConnectAsync()
        {
            InitSocket();
            var tcs = new TaskCompletionSource<bool>();
            _socket.BeginConnect(_endPoint, iar =>
            {
                _socket.EndConnect(iar);
                tcs.SetResult(_socket.Connected);
            }, null);
            return tcs.Task;
        }

        public bool SendAsync(SocketAsyncEventArgs args)
        {
            return _socket.SendAsync(args);
        }

        public Stream GetStream()
        {
            Stream netStream = new NetworkStream(_socket);

            if (!_ssl) return netStream;

            var sslStream = new SslStream(netStream, true);
            sslStream.AuthenticateAsClient(GetHostForAuthentication());
            return sslStream;
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

        string GetHostForAuthentication()
        {
            if (_endPoint == null)
                throw new ArgumentNullException("Remote endpoint is not set");
            else if (_endPoint is DnsEndPoint)
                return (_endPoint as DnsEndPoint).Host;
            else if (_endPoint is IPEndPoint)
                return (_endPoint as IPEndPoint).Address.ToString();

            throw new InvalidOperationException("Cannot get remote host");
        }
    }
}
