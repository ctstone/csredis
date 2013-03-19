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

        public RedisSentinelManager(params string[] sentinels)
        {
            _sentinels = new LinkedList<KeyValuePair<string, int>>();
            foreach (var host in sentinels)
            {
                string[] parts = host.Split(':');
                string hostname = parts[0];
                int port = Int32.Parse(parts[1]);
                _sentinels.AddLast(new KeyValuePair<string, int>(hostname, port));
            }
        }

        public RedisClient GetMaster(string masterName, int timeout, int masterTimeout)
        {
            foreach (var sentinel in GetSentinels(timeout))
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
        public RedisClient GetSlave(string masterName, int timeout, int slaveTimeout)
        {
            foreach (var s in GetSentinels(timeout))
            {
                using (s)
                {
                    foreach (var slave in s.Slaves(masterName))
                    {
                        if (slave != null)
                            return new RedisClient(slave["ip"], Int32.Parse(slave["port"]), slaveTimeout);
                    }
                }
            }
            return null;
        }

        public IEnumerable<RedisSentinelClient> GetSentinels(int timeout)
        {
            int c = _sentinels.Count;
            while (c-- > 0)
            {
                var first = _sentinels.First;
                RedisSentinelClient sentinel = new RedisSentinelClient(first.Value.Key, first.Value.Value, timeout);
                if (sentinel.Connected)
                    yield return sentinel;

                _sentinels.RemoveFirst();
                _sentinels.AddLast(first.Value);
            }
        }

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

            throw new Exception("Could not connect to any sentinel");
        }

        public void Add(string host, int port)
        {
            _sentinels.AddLast(new KeyValuePair<string, int>(host, port));
        }
    }
}
