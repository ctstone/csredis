using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisHyperLogLogTests : RedisTestBase
    {
        [TestMethod, TestCategory("HyperLogLog")]
        public void TestPfAdd()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Assert.AreEqual(1, Redis.PfAdd("test1", "a", "b", "c", "d", "e", "f", "g"));
            }
        }

        [TestMethod, TestCategory("HyperLogLog")]
        public void TestPfCount()
        {
            using (new RedisTestKeys(Redis, "test1", "test2"))
            {
                Assert.AreEqual(1, Redis.PfAdd("test1", "foo", "bar", "zap"));
                Assert.AreEqual(0, Redis.PfAdd("test1", "zap", "zap", "zap"));
                Assert.AreEqual(0, Redis.PfAdd("test1", "foo", "bar"));
                Assert.AreEqual(3, Redis.PfCount("test1"));
                Assert.AreEqual(1, Redis.PfAdd("test2", 1, 2, 3));
                Assert.AreEqual(6, Redis.PfCount("test1", "test2"));
            }
        }

        [TestMethod, TestCategory("HyperLogLog")]
        public void TestPfMerge()
        {
            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Assert.AreEqual(1, Redis.PfAdd("test1", "foo", "bar", "zap", "a"));
                Assert.AreEqual(1, Redis.PfAdd("test2", "a", "b", "c", "foo"));
                Assert.AreEqual("OK", Redis.PfMerge("test3", "test1", "test2"));
                Assert.AreEqual(6, Redis.PfCount("test3"));
            }
        }
    }
}
