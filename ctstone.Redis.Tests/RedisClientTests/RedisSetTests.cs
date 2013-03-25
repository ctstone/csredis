using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisSetTests : RedisTestBase
    {
        [TestMethod, TestCategory("Sets")]
        public void TestSAdd()
        {
            _redis.Del("test");

            Assert.AreEqual(2, _redis.SAdd("test", "Hello", "World"));
            Assert.AreEqual(0, _redis.SAdd("test", "World"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSCard()
        {
            _redis.Del("test");

            _redis.SAdd("test", "Hello", "World");
            Assert.AreEqual(2, _redis.SCard("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiff()
        {
            _redis.Del("test1", "test2");

            _redis.SAdd("test1", "a", "b", "c");
            _redis.SAdd("test2", "c", "d", "e");
            var resp1 = _redis.SDiff("test1", "test2");
            Assert.AreEqual(2, resp1.Length);
            Assert.IsTrue(resp1.Contains("a"));
            Assert.IsTrue(resp1.Contains("b"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiffStore()
        {
            _redis.Del("test1", "test2", "test3");

            _redis.SAdd("test1", "a", "b", "c");
            _redis.SAdd("test2", "c", "d", "e");
            Assert.AreEqual(2, _redis.SDiffStore("test3", "test1", "test2"));

            _redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSInter()
        {
            _redis.Del("test1", "test2");

            _redis.SAdd("test1", "a", "b", "c");
            _redis.SAdd("test2", "c", "d", "e");
            var resp = _redis.SInter("test1", "test2");
            Assert.AreEqual(1, resp.Length);
            Assert.IsTrue(resp.Contains("c"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSInterStore()
        {
            _redis.Del("test1", "test2", "test3");

            _redis.SAdd("test1", "a", "b", "c");
            _redis.SAdd("test2", "c", "d", "e");
            Assert.AreEqual(1, _redis.SInterStore("test3", "test1", "test2"));

            _redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSIsMember()
        {
            _redis.Del("test1", "test2");

            _redis.SAdd("test1", "a", "b", "c");
            Assert.IsTrue(_redis.SIsMember("test1", "a"));
            Assert.IsFalse(_redis.SIsMember("test1", "d"));
            Assert.IsFalse(_redis.SIsMember("test2", "a"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMembers()
        {
            _redis.Del("test1", "test2");

            _redis.SAdd("test1", "a", "b", "c");
            var resp1 = _redis.SMembers("test1");
            Assert.AreEqual(3, resp1.Length);

            var resp2 = _redis.SMembers("test2");
            Assert.IsNotNull(resp2);
            Assert.AreEqual(0, resp2.Length);

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMove()
        {
            _redis.Del("test1", "test2", "test3");

            _redis.SAdd("test1", "a", "b");
            _redis.SAdd("test2", "c");
            Assert.IsTrue(_redis.SMove("test1", "test2", "b"));
            Assert.IsFalse(_redis.SMove("test1", "test2", "b"));
            Assert.IsFalse(_redis.SMove("test3", "test2", "b"));

            _redis.Del("test1", "test2", "test3");
        }


        [TestMethod, TestCategory("Sets")]
        public void TestSPop()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.SAdd("test1", "a", "b", "c");
                Assert.IsNotNull(_redis.SPop("test1"));
                Assert.IsNotNull(_redis.SPop("test1"));
                Assert.IsNotNull(_redis.SPop("test1"));
                Assert.IsNull(_redis.SPop("test1"));
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRandomMember()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.SAdd("test1", "a", "b", "c");
                Assert.IsNotNull(_redis.SRandMember("test1"));
                Assert.AreEqual(2, _redis.SRandMember("test1", 2).Length);
                Assert.AreEqual(3, _redis.SRandMember("test1", 4).Length);
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRem()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.SAdd("test1", "a", "b", "c");
                Assert.AreEqual(1, _redis.SRem("test1", "a"));
                Assert.AreEqual(2, _redis.SRem("test1", "b", "c"));
                Assert.AreEqual(0, _redis.SRem("test1", "d"));
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnion()
        {
            using (new RedisTestKeys(_redis, "test1", "test2"))
            {
                _redis.SAdd("test1", "a", "b", "c");
                _redis.SAdd("test2", "c", "d", "e");
                Assert.AreEqual(5, _redis.SUnion("test1", "test2").Length);
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnionStore()
        {
            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.SAdd("test1", "a", "b", "c");
                _redis.SAdd("test2", "c", "d", "e");
                Assert.AreEqual(5, _redis.SUnionStore("test3", "test1", "test2"));
            }
        }
    }

    
}
