using ctstone.Redis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    public partial class RedisSentinelClient
    {
        public Task<bool> ConnectAsync()
        {
            return _connection.ConnectAsync();
        }

        public Task<object> CallAsync(string command, params string[] args)
        {
            return WriteAsync(new RedisObject(command, args));
        }

        Task<T> WriteAsync<T>(RedisCommand<T> command)
        {
            return _connection.CallAsync(command);
        }

        #region sentinel
        public Task<string> PingAsync()
        {
            return WriteAsync(RedisCommand.Ping());
        }

        /// <summary>
        /// Get a list of monitored Redis masters
        /// </summary>
        /// <returns>Redis master info</returns>
        public Task<RedisMasterInfo[]> MastersAsync()
        {
            return WriteAsync(RedisCommand.Sentinel.Masters());
        }

        public Task<RedisMasterInfo> MasterAsync(string masterName)
        {
            return WriteAsync(RedisCommand.Sentinel.Master(masterName));
        }

        /// <summary>
        /// Get a list of other Sentinels known to the current Sentinel
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Sentinel hosts and ports</returns>
        public Task<RedisSentinelInfo[]> SentinelsAsync(string masterName)
        {
            return WriteAsync(RedisCommand.Sentinel.Sentinels(masterName));
        }


        /// <summary>
        /// Get a list of monitored Redis slaves to the given master 
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Redis slave info</returns>
        public Task<RedisSlaveInfo[]> SlavesAsync(string masterName)
        {
            return WriteAsync(RedisCommand.Sentinel.Slaves(masterName));
        }

        /// <summary>
        /// Get the IP and port of the current master Redis server
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>IP and port of master Redis server</returns>
        public Task<Tuple<string, int>> GetMasterAddrByNameAsync(string masterName)
        {
            return WriteAsync(RedisCommand.Sentinel.GetMasterAddrByName(masterName));
        }

        public Task<RedisMasterState> IsMasterDownByAddrAsync(string ip, int port, long currentEpoch, string runId)
        {
            return WriteAsync(RedisCommand.Sentinel.IsMasterDownByAddr(ip, port, currentEpoch, runId));
        }

        public Task<long> ResetAsync(string pattern)
        {
            return WriteAsync(RedisCommand.Sentinel.Reset(pattern));
        }

        public Task<string> FailoverAsync(string masterName)
        {
            return WriteAsync(RedisCommand.Sentinel.Failover(masterName));
        }

        public Task<string> MonitorAsync(string name, int port, int quorum)
        {
            return WriteAsync(RedisCommand.Sentinel.Monitor(name, port, quorum));
        }

        public Task<string> RemoveAsync(string name)
        {
            return WriteAsync(RedisCommand.Sentinel.Remove(name));
        }

        public Task<string> SetAsync(string masterName, string option, string value)
        {
            return WriteAsync(RedisCommand.Sentinel.Set(masterName, option, value));
        }
        #endregion
    }
}
