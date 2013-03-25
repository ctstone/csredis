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
            _redis.Del("test");
            Assert.IsNull(_redis.BLPop(2, "test"));

            _redis.LPush("test", "first", "second");
            Assert.AreEqual("second", _redis.BLPop(2, "test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBLPopWithKey()
        {
            _redis.Del("test");
            Assert.IsNull(_redis.BLPopWithKey(2, "test"));

            _redis.LPush("test", "first", "second");
            var resp1 = _redis.BLPopWithKey(2, "test");
            Assert.IsNotNull(resp1);
            Assert.AreEqual("test", resp1.Item1);
            Assert.AreEqual("second", resp1.Item2);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPop()
        {
            _redis.Del("test");
            Assert.IsNull(_redis.BRPop(2, "test"));

            _redis.RPush("test", "first", "second");
            Assert.AreEqual("second", _redis.BRPop(2, "test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPopWithKey()
        {
            _redis.Del("test");
            Assert.IsNull(_redis.BRPopWithKey(2, "test"));

            _redis.RPush("test", "first", "second");
            var resp1 = _redis.BRPopWithKey(2, "test");
            Assert.IsNotNull(resp1);
            Assert.AreEqual("test", resp1.Item1);
            Assert.AreEqual("second", resp1.Item2);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestBRPopLPush()
        {
            _redis.Del("test1", "test2");
            Assert.IsNull(_redis.BRPopLPush("test1", "test2", 2));
            Assert.IsFalse(_redis.Exists("test2"));

            _redis.RPush("test1", "first", "second", "third");
            Assert.AreEqual(3, _redis.LLen("test1"));
            Assert.AreEqual("third", _redis.BRPopLPush("test1", "test2", 2));
            Assert.AreEqual(2, _redis.LLen("test1"));
            Assert.AreEqual(1, _redis.LLen("test2"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLIndex()
        {
            _redis.Del("test1", "test2");

            _redis.LPush("test1", "World", "Hello");
            Assert.AreEqual("Hello", _redis.LIndex("test1", 0));
            Assert.AreEqual("World", _redis.LIndex("test1", -1));
            Assert.IsNull(_redis.LIndex("test2", -1));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLInsert()
        {
            _redis.Del("test");

            // test before
            _redis.RPush("test", "Hello");
            _redis.RPush("test", "World");
            Assert.AreEqual(3, _redis.LInsert("test", RedisInsert.Before, "World", "There"));
            var resp1 = _redis.LRange("test", 0, -1);
            Assert.AreEqual("Hello", resp1[0]);
            Assert.AreEqual("There", resp1[1]);
            Assert.AreEqual("World", resp1[2]);

            _redis.Del("test");

            // test after
            _redis.RPush("test", "Hello");
            _redis.RPush("test", "World");
            Assert.AreEqual(3, _redis.LInsert("test", RedisInsert.After, "World", "!"));
            var resp2 = _redis.LRange("test", 0, -1);
            Assert.AreEqual("Hello", resp2[0]);
            Assert.AreEqual("World", resp2[1]);
            Assert.AreEqual("!", resp2[2]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLLen()
        {
            _redis.Del("test");

            _redis.LPush("test", "World", "Hello");
            Assert.AreEqual(2, _redis.LLen("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPop()
        {
            _redis.Del("test");

            _redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("one", _redis.LPop("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPush()
        {
            _redis.Del("test");

            Assert.AreEqual(1, _redis.LPush("test", "world"));
            Assert.AreEqual(3, _redis.LPush("test", "hello", "test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLPushX()
        {
            _redis.Del("test1", "test2");

            _redis.LPush("test1", "World");
            Assert.AreEqual(2, _redis.LPushX("test1", "Hello"));
            Assert.AreEqual(2, _redis.LLen("test1"));
            Assert.AreEqual(0, _redis.LPushX("test2", "Hello"));
            Assert.AreEqual(0, _redis.LLen("test2"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLRange()
        {
            _redis.Del("test");

            Assert.AreEqual(0, _redis.LRange("test", 0, -1).Length);

            _redis.RPush("test", "one", "two", "three");
            var resp1 = _redis.LRange("test", 0, 0);
            Assert.AreEqual(1, resp1.Length);
            Assert.AreEqual("one", resp1[0]);

            var resp2 = _redis.LRange("test", -3, 2);
            Assert.AreEqual(3, resp2.Length);
            Assert.AreEqual("one", resp2[0]);
            Assert.AreEqual("two", resp2[1]);
            Assert.AreEqual("three", resp2[2]);

            var resp3 = _redis.LRange("test", -100, 100);
            Assert.AreEqual(3, resp3.Length);
            Assert.AreEqual("one", resp3[0]);
            Assert.AreEqual("two", resp3[1]);
            Assert.AreEqual("three", resp3[2]);

            var resp4 = _redis.LRange("test", 5, 10);
            Assert.AreEqual(0, resp4.Length);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLRem()
        {
            _redis.Del("test");

            _redis.RPush("test", "hello", "hello", "foo", "hello");

            Assert.AreEqual(2, _redis.LRem("test", -2, "hello"));
            Assert.AreEqual(2, _redis.LLen("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLSet()
        {
            _redis.Del("test");

            _redis.RPush("test", "one", "two", "three");

            Assert.AreEqual("OK", _redis.LSet("test", 0, "four"));
            Assert.AreEqual("OK", _redis.LSet("test", -2, "five"));
            var resp1 = _redis.LRange("test", 0, -1);
            Assert.AreEqual(3, resp1.Length);
            Assert.AreEqual("four", resp1[0]);
            Assert.AreEqual("five", resp1[1]);
            Assert.AreEqual("three", resp1[2]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestLTrim()
        {
            _redis.Del("test");

            _redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("OK", _redis.LTrim("test", 1, -1));

            var resp1 = _redis.LRange("test", 0, -1);
            Assert.AreEqual(2, resp1.Length);
            Assert.AreEqual("two", resp1[0]);
            Assert.AreEqual("three", resp1[1]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPop()
        {
            _redis.Del("test");

            _redis.RPush("test", "one", "two", "three");
            Assert.AreEqual("three", _redis.RPop("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPopLPush()
        {
            _redis.Del("test1", "test2");
            Assert.IsNull(_redis.RPopLPush("test1", "test2"));
            Assert.IsFalse(_redis.Exists("test2"));

            _redis.RPush("test1", "first", "second", "third");
            Assert.AreEqual(3, _redis.LLen("test1"));
            Assert.AreEqual("third", _redis.RPopLPush("test1", "test2"));
            Assert.AreEqual(2, _redis.LLen("test1"));
            Assert.AreEqual(1, _redis.LLen("test2"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPush()
        {
            _redis.Del("test");

            Assert.AreEqual(1, _redis.RPush("test", "world"));
            Assert.AreEqual(3, _redis.RPush("test", "hello", "test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Lists")]
        public void TestRPushX()
        {
            _redis.Del("test1", "test2");

            _redis.RPush("test1", "World");
            Assert.AreEqual(2, _redis.RPushX("test1", "Hello"));
            Assert.AreEqual(2, _redis.LLen("test1"));
            Assert.AreEqual(0, _redis.RPushX("test2", "Hello"));
            Assert.AreEqual(0, _redis.LLen("test2"));

            _redis.Del("test");
        }
    }
}
