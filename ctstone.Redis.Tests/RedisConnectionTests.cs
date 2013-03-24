using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;

namespace ctstone.Redis.Tests
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
            Assert.AreEqual(echo, _redis.Echo(echo));
        }

        [TestMethod, TestCategory("Connection"), TestCategory("RedisClient")]
        public void TestPing()
        {
            Assert.AreEqual("PONG", _redis.Ping());
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
            using (new RedisTestKeys(_redis, test_key))
            {
                _redis.Set(test_key, test_value);
                Assert.AreEqual(test_value, _redis.Get(test_key));
                Assert.AreEqual("OK", _redis.Select(1));
                Assert.IsNull(_redis.Get(test_key));
                Assert.AreEqual("OK", _redis.Select(0));
                Assert.AreEqual(test_value, _redis.Get(test_key));
            }
        }
    }
}
