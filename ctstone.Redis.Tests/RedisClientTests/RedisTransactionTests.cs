using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisTransactionTests : RedisTestBase
    {
        [TestMethod, TestCategory("Transaction")]
        public void TestExec()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                Assert.AreEqual("OK", _redis.Multi());
                Assert.IsNull(_redis.Echo("asdf"));
                Assert.AreEqual(default(DateTime), _redis.Time());
                Assert.AreEqual(0, _redis.StrLen("test1"));
                Assert.IsNull(_redis.Set("test1", "asdf"));
                Assert.AreEqual(0, _redis.StrLen("test1"));
                var resp = _redis.Exec();
                Assert.AreEqual(5, resp.Length);
                Assert.IsTrue(_redis.Exists("test1"));
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestDiscard()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                Assert.AreEqual("OK", _redis.Multi());
                Assert.IsNull(_redis.Echo("asdf"));
                Assert.AreEqual(default(DateTime), _redis.Time());
                Assert.AreEqual(0, _redis.StrLen("test1"));
                Assert.IsNull(_redis.Set("test1", "asdf"));
                Assert.AreEqual(0, _redis.StrLen("test1"));
                Assert.AreEqual("OK", _redis.Discard());
                Assert.IsFalse(_redis.Exists("test1"));
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestWatch()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.Watch("test1");
                _redis.Multi();
                _redis.Set("test1", "multi");
                Assert.IsNotNull(_redis.Exec());
            }

            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.Watch("test1");
                _redis.Multi();
                using (var otherClient = new RedisClient(Host, Port, 0))
                {
                    otherClient.Auth(Password);
                    otherClient.Set("test1", "other");
                }
                _redis.Set("test1", "multi");
                Assert.IsNull(_redis.Exec());
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestUnwatch()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.Watch("test1");
                Assert.AreEqual("OK", _redis.Unwatch());
                _redis.Multi();
                using (var otherClient = new RedisClient(Host, Port, 0))
                {
                    otherClient.Auth(Password);
                    otherClient.Set("test1", "other");
                }
                _redis.Set("test1", "multi");
                Assert.IsNotNull(_redis.Exec());
            }
        }
    }
}
