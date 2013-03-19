using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;

namespace ctstone.Redis.Tests
{
    [TestClass]
    public class RedisConnectionTests : RedisTestBase
    {
        [TestMethod, TestCategory("Connection")]
        public void TestAuth()
        {
            using (var redis = new RedisClient(Host, Port, 500))
            {
                string result = redis.Auth(Password);
                Assert.AreEqual("OK", result);
            }
        }

        [TestMethod, TestCategory("Connection")]
        public void TestEcho()
        {
            string echo = Guid.NewGuid().ToString();
            string result = _redis.Echo(echo);
            Assert.AreEqual(echo, result);
        }

        [TestMethod, TestCategory("Connection")]
        public void TestPing()
        {
            string result = _redis.Ping();
            Assert.AreEqual("PONG", result);
        }

        // QUIT

        // SELECT
    }
}
