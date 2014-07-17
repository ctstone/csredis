using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis.Internal
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
}
