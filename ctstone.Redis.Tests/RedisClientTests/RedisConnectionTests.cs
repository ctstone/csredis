using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisConnectionTests : RedisTestBase
    {
        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestAuth()
        {
            using (var redis = new RedisClient(Host, Port, 0))
            {
                Assert.AreEqual("OK", redis.Auth(Password));
            }
        }

        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestEcho()
        {
            string echo = Guid.NewGuid().ToString();
            Assert.AreEqual(echo, Redis.Echo(echo));
        }

        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestPing()
        {
            Assert.AreEqual("PONG", Redis.Ping());
        }

        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestQuit()
        {
            using (var redis = new RedisClient(Host, Port, 0))
            {
                Assert.AreEqual("OK", redis.Quit());
                Assert.IsFalse(redis.Connected);
                try
                {
                    redis.Ping();
                }
                catch (Exception e)
                {
                    Assert.IsInstanceOfType(e, typeof(InvalidOperationException));
                }
            }
        }

        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestSelect()
        {
            string test_key = Guid.NewGuid().ToString();
            string test_value = "1";
            using (new RedisTestKeys(Redis, test_key))
            {
                Redis.Set(test_key, test_value);
                Assert.AreEqual(test_value, Redis.Get(test_key));
                Assert.AreEqual("OK", Redis.Select(1));
                Assert.IsNull(Redis.Get(test_key));
                Assert.AreEqual("OK", Redis.Select(0));
                Assert.AreEqual(test_value, Redis.Get(test_key));
            }
        }
    }
}
