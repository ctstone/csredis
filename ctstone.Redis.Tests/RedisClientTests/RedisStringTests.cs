using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;
using System.Collections.Generic;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisStringTests : RedisTestBase
    {
        [TestMethod, TestCategory("Strings")]
        public void TestAppend()
        {
            _redis.Del("test");
            var resp1 = _redis.Append("test", "Hello");
            Assert.AreEqual(5, resp1);

            var resp2 = _redis.Append("test", " World");
            Assert.AreEqual(11, resp2);

            var resp3 = _redis.Get("test");
            Assert.AreEqual("Hello World", resp3);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestBitCount()
        {
            _redis.Del("test");

            _redis.Set("test", "foobar");
            var resp1 = _redis.BitCount("test");
            Assert.AreEqual(26, resp1);

            var resp2 = _redis.BitCount("test", 0, 0);
            Assert.AreEqual(4, resp2);

            var resp3 = _redis.BitCount("test", 1, 1);
            Assert.AreEqual(6, resp3);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestBitOp()
        {
            _redis.Del("test1", "test2", "test3");

            _redis.Set("test1", "foobar");
            _redis.Set("test2", "abcdef");

            var resp1 = _redis.BitOp(RedisBitOp.And, "test3", "test1", "test2");
            Assert.AreEqual(6, resp1);

            _redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestDecr()
        {
            _redis.Del("test");

            _redis.Set("test", 10);
            var resp1 = _redis.Decr("test");
            Assert.AreEqual(9, resp1);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestDecrBy()
        {
            _redis.Del("test");

            _redis.Set("test", 10);
            var resp1 = _redis.DecrBy("test", 5);
            Assert.AreEqual(5, resp1);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGet()
        {
            _redis.Del("test");
            Assert.IsNull(_redis.Get("test"));

            _redis.Set("test", 1);
            Assert.AreEqual("1", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetBit()
        {
            _redis.Del("test");

            _redis.SetBit("test", 7, true);
            Assert.IsFalse(_redis.GetBit("test", 0));
            Assert.IsTrue(_redis.GetBit("test", 7));
            Assert.IsFalse(_redis.GetBit("test", 100));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetRange()
        {
            _redis.Del("test");

            _redis.Set("test", "This is a string");
            Assert.AreEqual("This", _redis.GetRange("test", 0, 3));
            Assert.AreEqual("ing", _redis.GetRange("test", -3, -1));
            Assert.AreEqual("This is a string", _redis.GetRange("test", 0, -1));
            Assert.AreEqual("string", _redis.GetRange("test", 10, 100));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetSet()
        {
            _redis.Del("test");

            _redis.Set("test", "Hello");
            Assert.AreEqual("Hello", _redis.GetSet("test", "World"));
            Assert.AreEqual("World", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncr()
        {
            _redis.Del("test");

            _redis.Set("test", 10);
            Assert.AreEqual(11, _redis.Incr("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncrBy()
        {
            _redis.Del("test");

            _redis.Set("test", 10);
            Assert.AreEqual(15, _redis.IncrBy("test", 5));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncrByFloat()
        {
            _redis.Del("test");

            _redis.Set("test", 10.50);
            Assert.AreEqual(10.6, _redis.IncrByFloat("test", 0.1));

            _redis.Set("test", "5.0e3");
            Assert.AreEqual(5200, _redis.IncrByFloat("test", 200));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMGet()
        {
            _redis.Del("test1", "test2", "test3");

            _redis.Set("test1", 1);
            _redis.Set("test2", 2);

            var resp1 = _redis.MGet("test1", "test2", "test3");
            Assert.AreEqual("1", resp1[0]);
            Assert.AreEqual("2", resp1[1]);
            Assert.IsNull(resp1[2]);

            _redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMSet()
        {
            // test array
            _redis.Del("test1", "test2", "test3");
            Assert.AreEqual("OK", _redis.MSet("test1", "v1", "test2", "v2", "test3", "v3"));
            Assert.AreEqual("v1", _redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));
            Assert.AreEqual("v3", _redis.Get("test3"));

            // test kvp
            _redis.Del("test1", "test2", "test3");
            Assert.AreEqual("OK", _redis.MSet(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
                Tuple.Create("test3", "v3"),
            }));
            Assert.AreEqual("v1", _redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));
            Assert.AreEqual("v3", _redis.Get("test3"));

            _redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMSetNx()
        {
            // test array
            _redis.Del("test1", "test2");
            Assert.IsTrue(_redis.MSetNx("test1", "v1", "test2", "v2"));
            Assert.AreEqual("v1", _redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));
            _redis.Del("test1");
            Assert.IsFalse(_redis.MSetNx("test1", "V1", "test2", "V2"));
            Assert.IsNull(_redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));

            // test kvp
            _redis.Del("test1", "test2");
            Assert.IsTrue(_redis.MSetNx(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
            }));
            Assert.AreEqual("v1", _redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));
            _redis.Del("test1");
            Assert.IsFalse(_redis.MSetNx(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
            }));
            Assert.IsNull(_redis.Get("test1"));
            Assert.AreEqual("v2", _redis.Get("test2"));

            _redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestPSetEx()
        {
            _redis.Del("test");

            Assert.AreEqual("OK", _redis.PSetEx("test", 10000, 1));
            Assert.AreEqual("1", _redis.Get("test"));
            Assert.IsTrue(_redis.PTtl("test") > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSet()
        {
            _redis.Del("test");

            Assert.AreEqual("OK", _redis.Set("test", 1));
            Assert.AreEqual("1", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetBit()
        {
            _redis.Del("test");

            Assert.IsFalse(_redis.SetBit("test", 7, true));
            Assert.IsTrue(_redis.SetBit("test", 7, false));
            Assert.AreEqual("\0", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetEx()
        {
            _redis.Del("test");

            Assert.AreEqual("OK", _redis.SetEx("test", 10, 1));
            Assert.AreEqual("1", _redis.Get("test"));
            Assert.IsTrue(_redis.Ttl("test") > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetNx()
        {
            _redis.Del("test");

            Assert.IsTrue(_redis.SetNx("test", "Hello"));
            Assert.IsFalse(_redis.SetNx("test", "World"));
            Assert.AreEqual("Hello", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetRange()
        {
            _redis.Del("test");

            _redis.Set("test", "Hello World");
            Assert.AreEqual(11, _redis.SetRange("test", 6, "Redis"));
            Assert.AreEqual("Hello Redis", _redis.Get("test"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestStrLen()
        {
            _redis.Del("test1", "test2");

            _redis.Set("test1", "Hello World");
            Assert.AreEqual(11, _redis.StrLen("test1"));
            Assert.AreEqual(0, _redis.StrLen("test2"));

            _redis.Del("test1", "test2");
        }
    }
}
