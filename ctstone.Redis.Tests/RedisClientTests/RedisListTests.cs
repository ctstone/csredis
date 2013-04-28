using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisListTests : RedisTestBase
    {
        [TestMethod, TestCategory("Lists")]
        public void TestBLPop()
        {
            Redis.Del("test");
            Assert.IsNull(Redis.BLPop(2, "test"));

            Redis.LPush("test", "first", "second");
            Assert.AreEqual("second", Redis.BLPop(2, "test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBLPopWithKey()
        {
            Redis.Del("test");
            Assert.IsNull(Redis.BLPopWithKey(2, "test"));

            Redis.LPush("test", "first", "second");
            var resp1 = Redis.BLPopWithKey(2, "test");
            Assert.IsNotNull(resp1);
            Assert.AreEqual("test", resp1.Item1);
            Assert.AreEqual("second", resp1.Item2);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPop()
        {
            Redis.Del("test");
            Assert.IsNull(Redis.BRPop(2, "test"));

            Redis.RPush("test", "first", "second");
            Assert.AreEqual("second", Redis.BRPop(2, "test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPopWithKey()
        {
            Redis.Del("test");
            Assert.IsNull(Redis.BRPopWithKey(2, "test"));

            Redis.RPush("test", "first", "second");
            var resp1 = Redis.BRPopWithKey(2, "test");
            Assert.IsNotNull(resp1);
            Assert.AreEqual("test", resp1.Item1);
            Assert.AreEqual("second", resp1.Item2);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPopLPush()
        {
            Redis.Del("test1", "test2");
            Assert.IsNull(Redis.BRPopLPush("test1", "test2", 2));
            Assert.IsFalse(Redis.Exists("test2"));

            Redis.RPush("test1", "first", "second", "third");
            Assert.AreEqual(3, Redis.LLen("test1"));
            Assert.AreEqual("third", Redis.BRPopLPush("test1", "test2", 2));
            Assert.AreEqual(2, Redis.LLen("test1"));
            Assert.AreEqual(1, Redis.LLen("test2"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLIndex()
        {
            Redis.Del("test1", "test2");

            Redis.LPush("test1", "World", "Hello");
            Assert.AreEqual("Hello", Redis.LIndex("test1", 0));
            Assert.AreEqual("World", Redis.LIndex("test1", -1));
            Assert.IsNull(Redis.LIndex("test2", -1));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLInsert()
        {
            Redis.Del("test");

            // test before
            Redis.RPush("test", "Hello");
            Redis.RPush("test", "World");
            Assert.AreEqual(3, Redis.LInsert("test", RedisInsert.Before, "World", "There"));
            var resp1 = Redis.LRange("test", 0, -1);
            Assert.AreEqual("Hello", resp1[0]);
            Assert.AreEqual("There", resp1[1]);
            Assert.AreEqual("World", resp1[2]);

            Redis.Del("test");

            // test after
            Redis.RPush("test", "Hello");
            Redis.RPush("test", "World");
            Assert.AreEqual(3, Redis.LInsert("test", RedisInsert.After, "World", "!"));
            var resp2 = Redis.LRange("test", 0, -1);
            Assert.AreEqual("Hello", resp2[0]);
            Assert.AreEqual("World", resp2[1]);
            Assert.AreEqual("!", resp2[2]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLLen()
        {
            Redis.Del("test");

            Redis.LPush("test", "World", "Hello");
            Assert.AreEqual(2, Redis.LLen("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPop()
        {
            Redis.Del("test");

            Redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("one", Redis.LPop("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPush()
        {
            Redis.Del("test");

            Assert.AreEqual(1, Redis.LPush("test", "world"));
            Assert.AreEqual(3, Redis.LPush("test", "hello", "test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPushX()
        {
            Redis.Del("test1", "test2");

            Redis.LPush("test1", "World");
            Assert.AreEqual(2, Redis.LPushX("test1", "Hello"));
            Assert.AreEqual(2, Redis.LLen("test1"));
            Assert.AreEqual(0, Redis.LPushX("test2", "Hello"));
            Assert.AreEqual(0, Redis.LLen("test2"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLRange()
        {
            Redis.Del("test");

            Assert.AreEqual(0, Redis.LRange("test", 0, -1).Length);

            Redis.RPush("test", "one", "two", "three");
            var resp1 = Redis.LRange("test", 0, 0);
            Assert.AreEqual(1, resp1.Length);
            Assert.AreEqual("one", resp1[0]);

            var resp2 = Redis.LRange("test", -3, 2);
            Assert.AreEqual(3, resp2.Length);
            Assert.AreEqual("one", resp2[0]);
            Assert.AreEqual("two", resp2[1]);
            Assert.AreEqual("three", resp2[2]);

            var resp3 = Redis.LRange("test", -100, 100);
            Assert.AreEqual(3, resp3.Length);
            Assert.AreEqual("one", resp3[0]);
            Assert.AreEqual("two", resp3[1]);
            Assert.AreEqual("three", resp3[2]);

            var resp4 = Redis.LRange("test", 5, 10);
            Assert.AreEqual(0, resp4.Length);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLRem()
        {
            Redis.Del("test");

            Redis.RPush("test", "hello", "hello", "foo", "hello");

            Assert.AreEqual(2, Redis.LRem("test", -2, "hello"));
            Assert.AreEqual(2, Redis.LLen("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLSet()
        {
            Redis.Del("test");

            Redis.RPush("test", "one", "two", "three");

            Assert.AreEqual("OK", Redis.LSet("test", 0, "four"));
            Assert.AreEqual("OK", Redis.LSet("test", -2, "five"));
            var resp1 = Redis.LRange("test", 0, -1);
            Assert.AreEqual(3, resp1.Length);
            Assert.AreEqual("four", resp1[0]);
            Assert.AreEqual("five", resp1[1]);
            Assert.AreEqual("three", resp1[2]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLTrim()
        {
            Redis.Del("test");

            Redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("OK", Redis.LTrim("test", 1, -1));

            var resp1 = Redis.LRange("test", 0, -1);
            Assert.AreEqual(2, resp1.Length);
            Assert.AreEqual("two", resp1[0]);
            Assert.AreEqual("three", resp1[1]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPop()
        {
            Redis.Del("test");

            Redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("three", Redis.RPop("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPopLPush()
        {
            Redis.Del("test1", "test2");
            Assert.IsNull(Redis.RPopLPush("test1", "test2"));
            Assert.IsFalse(Redis.Exists("test2"));

            Redis.RPush("test1", "first", "second", "third");
            Assert.AreEqual(3, Redis.LLen("test1"));
            Assert.AreEqual("third", Redis.RPopLPush("test1", "test2"));
            Assert.AreEqual(2, Redis.LLen("test1"));
            Assert.AreEqual(1, Redis.LLen("test2"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPush()
        {
            Redis.Del("test");

            Assert.AreEqual(1, Redis.RPush("test", "world"));
            Assert.AreEqual(3, Redis.RPush("test", "hello", "test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPushX()
        {
            Redis.Del("test1", "test2");

            Redis.RPush("test1", "World");
            Assert.AreEqual(2, Redis.RPushX("test1", "Hello"));
            Assert.AreEqual(2, Redis.LLen("test1"));
            Assert.AreEqual(0, Redis.RPushX("test2", "Hello"));
            Assert.AreEqual(0, Redis.LLen("test2"));

            Redis.Del("test");
        }
    }
}
