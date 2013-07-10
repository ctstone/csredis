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
            using (new RedisTestKeys(Redis, "test1"))
            {
                bool transaction_started = false;
                bool transaction_queued = false;

                Redis.TransactionStarted += (s, e) =>
                {
                    Assert.AreEqual("OK", e.Status);
                    transaction_started = true;
                };
                Redis.TransactionQueued += (s, e) =>
                {
                    Assert.AreEqual("QUEUED", e.Status);
                    transaction_queued = true;

                };

                Redis.Multi();
                Assert.IsNull(Redis.Echo("asdf"));
                Assert.AreEqual(default(DateTime), Redis.Time());
                Assert.AreEqual(0, Redis.StrLen("test1"));
                Assert.IsNull(Redis.Set("test1", "asdf"));
                Assert.AreEqual(0, Redis.StrLen("test1"));

                var resp = Redis.Exec();
                Assert.AreEqual(5, resp.Length);
                Assert.IsTrue(Redis.Exists("test1"));

                Assert.IsTrue(transaction_started);
                Assert.IsTrue(transaction_queued);
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestDiscard()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Multi();
                Assert.IsNull(Redis.Echo("asdf"));
                Assert.AreEqual(default(DateTime), Redis.Time());
                Assert.AreEqual(0, Redis.StrLen("test1"));
                Assert.IsNull(Redis.Set("test1", "asdf"));
                Assert.AreEqual("OK", Redis.Discard());
                Assert.IsFalse(Redis.Exists("test1"));
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestWatch()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Watch("test1");
                Redis.Multi();
                Redis.Set("test1", "multi");
                Assert.IsNotNull(Redis.Exec());
            }

            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Watch("test1");
                Redis.Multi();
                using (var otherClient = new RedisClient(Host, Port, 0))
                {
                    otherClient.Auth(Password);
                    otherClient.Set("test1", "other");
                }
                Redis.Set("test1", "multi");
                Assert.IsNull(Redis.Exec());
            }
        }

        [TestMethod, TestCategory("Transaction")]
        public void TestUnwatch()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Watch("test1");
                Assert.AreEqual("OK", Redis.Unwatch());
                Redis.Multi();
                using (var otherClient = new RedisClient(Host, Port, 0))
                {
                    otherClient.Auth(Password);
                    otherClient.Set("test1", "other");
                }
                Redis.Set("test1", "multi");
                Assert.IsNotNull(Redis.Exec());
            }
        }
    }
}
