﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace CSRedis.Internal.IO
{
    class RedisSocket : IRedisSocket
    {
        readonly bool _ssl;
        Socket _socket;
        EndPoint _remote;

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

        public RedisSocket(bool ssl)
        {
            _ssl = ssl;
        }

        public void Connect(EndPoint endpoint)
        {
            InitSocket(endpoint);
            if (endpoint is DnsEndPoint)
            {
                var host = (endpoint as DnsEndPoint).Host;
                var port = (endpoint as DnsEndPoint).Port;
                _socket.Connect(host,port);
            }
            else if (endpoint is IPEndPoint) 
                _socket.Connect((endpoint as IPEndPoint));
        }

        public bool ConnectAsync(SocketAsyncEventArgs args)
        {
            InitSocket(args.RemoteEndPoint);
            return _socket.ConnectAsync(args);
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

        void InitSocket(EndPoint endpoint)
        {
            if (_socket != null)
                _socket.Dispose();

            _socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _remote = endpoint;
        }

        string GetHostForAuthentication()
        {
            if (_remote == null)
                throw new ArgumentNullException("Remote endpoint is not set");
            else if (_remote is DnsEndPoint)
                return (_remote as DnsEndPoint).Host;
            else if (_remote is IPEndPoint)
                return (_remote as IPEndPoint).Address.ToString();

            throw new InvalidOperationException("Cannot get remote host");
        }
    }
}
