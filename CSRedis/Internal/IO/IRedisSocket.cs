using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    interface IRedisSocket : IDisposable
    {
        EndPoint EndPoint { get; }
        bool SSL { get; }
        bool Connected { get; }
        int ReceiveTimeout { get; set; }
        int SendTimeout { get; set; }
        void Connect();
        bool ConnectAsync(SocketAsyncEventArgs args);
        Task<bool> ConnectAsync();
        bool SendAsync(SocketAsyncEventArgs args);
        Stream GetStream();
    }
}
