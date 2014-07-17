using ctstone.Redis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ctstone.Redis
{
    public partial class RedisSentinelClient
    {
        public bool Connect(int timeout)
        {
            return _connection.Connect(timeout);
        }

        /// <summary>
        /// Call arbitrary Sentinel command (e.g. for a command not yet implemented in this library)
        /// </summary>
        /// <param name="command">The name of the command</param>
        /// <param name="args">Array of arguments to the command</param>
        /// <returns>Redis unified response</returns>
        public object Call(string command, params string[] args)
        {
            return Write(RedisCommand.Call(command, args));
        }

        T Write<T>(RedisCommand<T> command)
        {
            return _connection.Call(command);
        }

        #region sentinel
        /// <summary>
        /// Ping the Sentinel server
        /// </summary>
        /// <returns>Status code</returns>
        public string Ping()
        {
            return Write(RedisCommand.Ping());
        }

        /// <summary>
        /// Get a list of monitored Redis masters
        /// </summary>
        /// <returns>Redis master info</returns>
        public RedisMasterInfo[] Masters()
        {
            return Write(RedisCommand.Sentinel.Masters());
        }

        public RedisMasterInfo Master(string masterName)
        {
            return Write(RedisCommand.Sentinel.Master(masterName));
        }

        /// <summary>
        /// Get a list of other Sentinels known to the current Sentinel
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Sentinel hosts and ports</returns>
        public RedisSentinelInfo[] Sentinels(string masterName)
        {
            return Write(RedisCommand.Sentinel.Sentinels(masterName));
        }


        /// <summary>
        /// Get a list of monitored Redis slaves to the given master 
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>Redis slave info</returns>
        public RedisSlaveInfo[] Slaves(string masterName)
        {
            return Write(RedisCommand.Sentinel.Slaves(masterName));
        }

        /// <summary>
        /// Get the IP and port of the current master Redis server
        /// </summary>
        /// <param name="masterName">Name of monitored master</param>
        /// <returns>IP and port of master Redis server</returns>
        public Tuple<string, int> GetMasterAddrByName(string masterName)
        {
            return Write(RedisCommand.Sentinel.GetMasterAddrByName(masterName));
        }

        /// <summary>
        /// Open one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channels">Name of channels to open (refer to http://redis.io/ for channel names)</param>
        public void Subscribe(params string[] channels)
        {
            _subscription.Send(RedisCommand.Subscribe(channels));
        }

        /// <summary>
        /// Close one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channels">Name of channels to close</param>
        public void Unsubscribe(params string[] channels)
        {
            _subscription.Send(RedisCommand.Unsubscribe(channels));
        }

        /// <summary>
        /// Open one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channelPatterns">Pattern of channels to open (refer to http://redis.io/ for channel names)</param>
        public void PSubscribe(params string[] channelPatterns)
        {
            _subscription.Send(RedisCommand.PSubscribe(channelPatterns));
        }

        /// <summary>
        /// Close one or more subscription channels to Redis Sentinel server
        /// </summary>
        /// <param name="channelPatterns">Pattern of channels to close</param>
        public void PUnsubscribe(params string[] channelPatterns)
        {
            _subscription.Send(RedisCommand.PUnsubscribe(channelPatterns));
        }

        public RedisMasterState IsMasterDownByAddr(string ip, int port, long currentEpoch, string runId)
        {
            return Write(RedisCommand.Sentinel.IsMasterDownByAddr(ip, port, currentEpoch, runId));
        }

        public long Reset(string pattern)
        {
            return Write(RedisCommand.Sentinel.Reset(pattern));
        }

        public string Failover(string masterName)
        {
            return Write(RedisCommand.Sentinel.Failover(masterName));
        }

        public string Monitor(string name, int port, int quorum)
        {
            return Write(RedisCommand.Sentinel.Monitor(name, port, quorum));
        }

        public string Remove(string name)
        {
            return Write(RedisCommand.Sentinel.Remove(name));
        }

        public string Set(string masterName, string option, string value)
        {
            return Write(RedisCommand.Sentinel.Set(masterName, option, value));
        }
        #endregion
    }
}
