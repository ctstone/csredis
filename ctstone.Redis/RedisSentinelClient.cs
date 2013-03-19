using System;
using System.Collections.Generic;

namespace ctstone.Redis
{
    public class RedisSentinelClient : IDisposable
    {
        private RedisConnection _connection;

        public bool Connected { get { return _connection.Connected; } }

        public RedisSentinelClient(string host, int port, int timeoutMilliseconds)
        {
            _connection = new RedisConnection(host, port);
            _connection.Connect(timeoutMilliseconds);
        }

        public string Ping()
        {
            return Write(RedisCommand.Ping());
        }

        public object Masters()
        {
            return Write(RedisCommand.Masters());
        }

        public Dictionary<string, string>[] Sentinels(string masterName)
        {
            return Write(RedisCommand.Sentinels(masterName));
        }

        public Dictionary<string, string>[] Slaves(string masterName)
        {
            return Write(RedisCommand.Slaves(masterName));
        }

        public Tuple<string, int> GetMasterAddrByName(string masterName)
        {
            return Write(RedisCommand.GetMasterAddrByName(masterName));
        }

        private T Write<T>(RedisCommand<T> command)
        {
            if (!_connection.Connected)
                throw new InvalidOperationException("RedisSentinel is not connected");

            return _connection.Call(command.Parser, command.Command, command.Arguments);
        }

        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }
    }
}
