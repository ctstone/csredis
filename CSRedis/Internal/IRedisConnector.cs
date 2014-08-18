using CSRedis.Internal.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    interface IRedisConnector : IDisposable
    {
        bool IsConnected { get; }
        string Host { get; }
        int Port { get; }
        int ReceiveTimeout { get; set; }
        int SendTimeout { get; set; }
        Encoding Encoding { get; set; }
        int ReconnectAttempts { get; set; }
        int ReconnectWait { get; set; }
        bool IsPipelined { get; }

        event EventHandler Connected;

        bool Connect();
        Task<bool> ConnectAsync();
        T Call<T>(RedisCommand<T> command);
        Task<T> CallAsync<T>(RedisCommand<T> command);
        void Write(RedisCommand command);
        T Read<T>(Func<RedisReader, T> func);
        void Read(Stream destination, int bufferSize);
        void BeginPipe();
        object[] EndPipe();
    }
}
