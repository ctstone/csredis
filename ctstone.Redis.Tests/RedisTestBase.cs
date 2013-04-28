using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Configuration;

namespace ctstone.Redis.Tests.RedisClientTests
{
    public abstract class RedisTestBase
    {
        protected string Host;
        protected int Port;
        protected string Password;
        private Lazy<RedisClient> _redis;
        private Lazy<RedisClientAsync> _async;

        protected RedisClient Redis { get { return _redis.Value; } }
        protected RedisClientAsync Async { get { return _async.Value; } }

        [TestInitialize]
        public void Initialize()
        {
            Host = ConfigurationManager.AppSettings["host"];
            Port = Int32.Parse(ConfigurationManager.AppSettings["port"]);
            Password = ConfigurationManager.AppSettings["password"];

            _redis = new Lazy<RedisClient>(() =>
            {
                RedisClient redis = new RedisClient(Host, Port, 0);
                redis.Auth(Password);
                return redis;
            });

            _async = new Lazy<RedisClientAsync>(() =>
            {
                RedisClientAsync async = new RedisClientAsync(Host, Port, 0);
                async.Auth(Password).Wait();
                return async;
            });
        }

        [TestCleanup]
        public void Cleanup()
        {
            if (_async.IsValueCreated)
                _async.Value.Dispose();

            if (_redis.IsValueCreated)
                _redis.Value.Dispose();
        }
    }
}
