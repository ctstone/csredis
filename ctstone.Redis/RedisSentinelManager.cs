using System;
using System.Collections.Generic;

namespace ctstone.Redis
{
    /// <summary>
    /// Manage Redis Sentinel connections
    /// </summary>
    public class RedisSentinelManager
    {
        private LinkedList<KeyValuePair<string, int>> _sentinels;

        /// <summary>
        /// Instantiate a new instance of the RedisSentinelManager class
        /// </summary>
        /// <param name="sentinels">array of Sentinel nodes ["host1:ip", "host2:ip", ..]</param>
        public RedisSentinelManager(params string[] sentinels)
        {
            _sentinels = new LinkedList<KeyValuePair<string, int>>();
            foreach (var host in sentinels)
            {
                string[] parts = host.Split(':');
                string hostname = parts[0].Trim();
                int port = Int32.Parse(parts[1]);
                _sentinels.AddLast(new KeyValuePair<string, int>(hostname, port));
            }
        }

        /// <summary>
        /// Connect to and return the active master Redis client
        /// </summary>
        /// <param name="masterName">Name of master</param>
        /// <param name="timeout">Time to wait for Sentinel response (milliseconds)</param>
        /// <param name="masterTimeout">Time to wait for Redis master response (milliseconds)</param>
        /// <returns>Connected RedisClient master, or null if cannot connect</returns>
        public RedisClient GetMaster(string masterName, int timeout, int masterTimeout)
        {
            RedisSentinelClient sentinel = GetSentinel(timeout);
            if (sentinel != null)
            {
                using (sentinel)
                {
                    var host = sentinel.GetMasterAddrByName(masterName);
                    if (host != null)
                        return new RedisClient(host.Item1, host.Item2, masterTimeout);
                }
            }

            return null;
        }

        /// <summary>
        /// Connect to and return a Redis slave client
        /// </summary>
        /// <param name="masterName">Name of master that slave belongs to</param>
        /// <param name="timeout">Time to wait for Sentinel response (milliseconds)</param>
        /// <param name="slaveTimeout">Time to wait for Redis slave response (milliseconds)</param>
        /// <returns>Connected RedisClient slave, or null if cannot connect</returns>
        public RedisClient GetSlave(string masterName, int timeout, int slaveTimeout)
        {
            RedisSentinelClient sentinel = GetSentinel(timeout);
            if (sentinel != null)
            {
                using (sentinel)
                {
                    foreach (var slave in sentinel.Slaves(masterName))
                    {
                        if (slave != null)
                            return new RedisClient(slave.Ip, slave.Port, slaveTimeout);
                    }
                }
            }
            
            return null;
        }

        /// <summary>
        /// Connect to and return a Redis Sentinel client
        /// </summary>
        /// <param name="timeout">Time to wait for Sentinel response (milliseconds)</param>
        /// <returns>Connected Sentinel client, or null if cannot connect</returns>
        public RedisSentinelClient GetSentinel(int timeout)
        {
            int c = _sentinels.Count;
            while (c-- > 0)
            {
                var first = _sentinels.First;
                RedisSentinelClient sentinel = new RedisSentinelClient(first.Value.Key, first.Value.Value, timeout);
                if (sentinel.Connected)
                    return sentinel;

                _sentinels.RemoveFirst();
                _sentinels.AddLast(first.Value);
            }

            return null;
        }

        /// <summary>
        /// Add a new Sentinel server to known hosts
        /// </summary>
        /// <param name="host">Sentinel server hostname or IP</param>
        /// <param name="port">Sentinel server port</param>
        public void Add(string host, int port)
        {
            _sentinels.AddLast(new KeyValuePair<string, int>(host, port));
        }
    }
}
