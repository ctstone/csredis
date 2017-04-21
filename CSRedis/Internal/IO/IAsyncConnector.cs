using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal.IO
{
    interface IAsyncConnector : IDisposable
    {
        Task<bool> ConnectAsync();
        Task<T> CallAsync<T>(RedisCommand<T> command);
        event EventHandler Connected;
    }
}
