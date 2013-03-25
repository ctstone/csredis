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
        protected RedisClient _redis;
        protected RedisClientAsync _async;

        [TestInitialize]
        public void Initialize()
        {
            Host = ConfigurationManager.AppSettings["host"];
            Port = Int32.Parse(ConfigurationManager.AppSettings["port"]);
            Password = ConfigurationManager.AppSettings["password"];
            _redis = new RedisClient(Host, Port, 0);
            _redis.Auth(Password);

            _async = new RedisClientAsync(Host, Port, 0);
            _async.Auth(Password).Wait();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _redis.Dispose();
        }
    }
}
