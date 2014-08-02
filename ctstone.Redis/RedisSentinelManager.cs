using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

// http://redis.io/topics/sentinel-clients

namespace CSRedis
{
    /// <summary>
    /// Represents a managed connection to a Redis master instance via a set of Redis sentinel nodes
    /// </summary>
    public class RedisSentinelManager : IDisposable
    {
        readonly LinkedList<Tuple<string, int>> _sentinels;
        string _masterName;
        string _auth;
        int _connectTimeout;
        RedisClient _redisClient;

        /// <summary>
        /// Raised when the connection has sucessfully reconnected
        /// </summary>
        public event Action Reconnected;

        /// <summary>
        /// Create a new RedisSentinenlManager
        /// </summary>
        /// <param name="sentinels">Sentinel addresses (host:ip)</param>
        public RedisSentinelManager(params string[] sentinels)
        {
            _sentinels = new LinkedList<Tuple<string, int>>();
            foreach (var host in sentinels)
            {
                string[] parts = host.Split(':');
                string hostname = parts[0].Trim();
                int port = Int32.Parse(parts[1]);
                _sentinels.AddLast(Tuple.Create(hostname, port));
            }
        }

        /// <summary>
        /// Obtain connection to the specified master node
        /// </summary>
        /// <param name="masterName">Name of Redis master</param>
        /// <param name="auth">Redis master password</param>
        /// <param name="timeout">Connection timeout (milliseconds)</param>
        public void Connect(string masterName, string auth = null, int timeout = 200)
        {
            _masterName = masterName;
            _connectTimeout = timeout;
            _auth = auth;

            if (!SetMaster(masterName, timeout))
                throw new IOException("Could not connect to sentinel or master");

            _redisClient.ReconnectAttempts = 0;
        }

        /// <summary>
        /// Execute command against the master, reconnecting if necessary
        /// </summary>
        /// <typeparam name="T">Command return type</typeparam>
        /// <param name="redisAction">Command to execute</param>
        /// <returns>Command result</returns>
        public T Call<T>(Func<RedisClient, T> redisAction)
        {
            if (_masterName == null)
                throw new InvalidOperationException("Master not set");

            try
            {
                return redisAction(_redisClient);
            }
            catch (IOException)
            {
                Next();
                Connect(_masterName, _auth, _connectTimeout);
                return Call(redisAction);
            }
        }

        /// <summary>
        /// Release resources held by the current RedisSentinelManager
        /// </summary>
        public void Dispose()
        {
            if (_redisClient != null)
                _redisClient.Dispose();
        }

        bool SetMaster(string name, int timeout)
        {
            for (int i = 0; i < _sentinels.Count; i++)
            {
                using (var sentinel = Current())
                {
                    if (!sentinel.Connect(timeout))
                        continue;

                    var master = sentinel.GetMasterAddrByName(name);
                    if (master == null)
                        continue;

                    _redisClient = new RedisClient(master.Item1, master.Item2);
                    _redisClient.Reconnected += OnConnectionReconnected;
                    if (!_redisClient.Connect(timeout))
                        continue;

                    if (_auth != null)
                        _redisClient.Auth(_auth);

                    var role = _redisClient.Role();
                    if (role.RoleName == "master")
                        return true;
                }

                Next();
            }
            return false;
        }

        RedisSentinelClient Current()
        {
            return new RedisSentinelClient(_sentinels.First.Value.Item1, _sentinels.First.Value.Item2); ;
        }

        void Next()
        {
            var first = _sentinels.First;
            _sentinels.RemoveFirst();
            _sentinels.AddLast(first.Value);
        }

        void OnConnectionReconnected()
        {
            if (Reconnected != null)
                Reconnected();
        }
    }
}