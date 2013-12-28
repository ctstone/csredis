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
            Redis.Del("test");

            Assert.AreEqual(2, Redis.SAdd("test", "Hello", "World"));
            Assert.AreEqual(0, Redis.SAdd("test", "World"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSCard()
        {
            Redis.Del("test");

            Redis.SAdd("test", "Hello", "World");
            Assert.AreEqual(2, Redis.SCard("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiff()
        {
            Redis.Del("test1", "test2");

            Redis.SAdd("test1", "a", "b", "c");
            Redis.SAdd("test2", "c", "d", "e");
            var resp1 = Redis.SDiff("test1", "test2");
            Assert.AreEqual(2, resp1.Length);
            Assert.IsTrue(resp1.Contains("a"));
            Assert.IsTrue(resp1.Contains("b"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiffStore()
        {
            Redis.Del("test1", "test2", "test3");

            Redis.SAdd("test1", "a", "b", "c");
            Redis.SAdd("test2", "c", "d", "e");
            Assert.AreEqual(2, Redis.SDiffStore("test3", "test1", "test2"));

            Redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSInter()
        {
            Redis.Del("test1", "test2");

            Redis.SAdd("test1", "a", "b", "c");
            Redis.SAdd("test2", "c", "d", "e");
            var resp = Redis.SInter("test1", "test2");
            Assert.AreEqual(1, resp.Length);
            Assert.IsTrue(resp.Contains("c"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSInterStore()
        {
            Redis.Del("test1", "test2", "test3");

            Redis.SAdd("test1", "a", "b", "c");
            Redis.SAdd("test2", "c", "d", "e");
            Assert.AreEqual(1, Redis.SInterStore("test3", "test1", "test2"));

            Redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSIsMember()
        {
            Redis.Del("test1", "test2");

            Redis.SAdd("test1", "a", "b", "c");
            Assert.IsTrue(Redis.SIsMember("test1", "a"));
            Assert.IsFalse(Redis.SIsMember("test1", "d"));
            Assert.IsFalse(Redis.SIsMember("test2", "a"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMembers()
        {
            Redis.Del("test1", "test2");

            Redis.SAdd("test1", "a", "b", "c");
            var resp1 = Redis.SMembers("test1");
            Assert.AreEqual(3, resp1.Length);

            var resp2 = Redis.SMembers("test2");
            Assert.IsNotNull(resp2);
            Assert.AreEqual(0, resp2.Length);

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMove()
        {
            Redis.Del("test1", "test2", "test3");

            Redis.SAdd("test1", "a", "b");
            Redis.SAdd("test2", "c");
            Assert.IsTrue(Redis.SMove("test1", "test2", "b"));
            Assert.IsFalse(Redis.SMove("test1", "test2", "b"));
            Assert.IsFalse(Redis.SMove("test3", "test2", "b"));

            Redis.Del("test1", "test2", "test3");
        }


        [TestMethod, TestCategory("Sets")]
        public void TestSPop()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.SAdd("test1", "a", "b", "c");
                Assert.IsNotNull(Redis.SPop("test1"));
                Assert.IsNotNull(Redis.SPop("test1"));
                Assert.IsNotNull(Redis.SPop("test1"));
                Assert.IsNull(Redis.SPop("test1"));
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRandomMember()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.SAdd("test1", "a", "b", "c");
                Assert.IsNotNull(Redis.SRandMember("test1"));
                Assert.AreEqual(2, Redis.SRandMember("test1", 2).Length);
                Assert.AreEqual(3, Redis.SRandMember("test1", 4).Length);
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRem()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.SAdd("test1", "a", "b", "c");
                Assert.AreEqual(1, Redis.SRem("test1", "a"));
                Assert.AreEqual(2, Redis.SRem("test1", "b", "c"));
                Assert.AreEqual(0, Redis.SRem("test1", "d"));
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnion()
        {
            using (new RedisTestKeys(Redis, "test1", "test2"))
            {
                Redis.SAdd("test1", "a", "b", "c");
                Redis.SAdd("test2", "c", "d", "e");
                Assert.AreEqual(5, Redis.SUnion("test1", "test2").Length);
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnionStore()
        {
            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.SAdd("test1", "a", "b", "c");
                Redis.SAdd("test2", "c", "d", "e");
                Assert.AreEqual(5, Redis.SUnionStore("test3", "test1", "test2"));
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSScan()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.SAdd("test1", 1, 2, 3, 4);
                var scan = Redis.SScan("test1", 0);
                Assert.AreEqual(4, scan.Items.Length);
                Assert.AreEqual(0, scan.Cursor);
            }
        }
    }

    
}
