using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Configuration;

namespace ctstone.Redis.Tests
{
    public abstract class RedisTestBase
    {
        protected string Host;
        protected int Port;
        protected string Password;
        protected RedisClient _redis;

        [TestInitialize]
        public void Initialize()
        {
            Host = ConfigurationManager.AppSettings["host"];
            Port = Int32.Parse(ConfigurationManager.AppSettings["port"]);
            Password = ConfigurationManager.AppSettings["password"];
            _redis = new RedisClient(Host, Port, 3000);
            _redis.Auth(Password);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _redis.Dispose();
        }
    }
}
