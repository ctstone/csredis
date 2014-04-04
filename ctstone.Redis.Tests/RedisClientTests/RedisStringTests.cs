using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;
using System.Collections.Generic;
using System.Text;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisStringTests : RedisTestBase
    {
        [TestMethod, TestCategory("Strings")]
        public void TestUTF8()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                string bytes1 = Encoding.UTF8.GetString(new byte[] { 0x24 });
                Redis.Set("test1", bytes1);
                Assert.AreEqual("$", Redis.Get("test1"));

                string bytes2 = Encoding.UTF8.GetString(new byte[] { 0xc2, 0xa2 });
                Redis.Set("test1", bytes2);
                Assert.AreEqual("¢", Redis.Get("test1"));

                string bytes3 = Encoding.UTF8.GetString(new byte[] { 0xe2, 0x82, 0xac });
                Redis.Set("test1", bytes3);
                Assert.AreEqual("€", Redis.Get("test1"));

                string bytes4 = Encoding.UTF8.GetString(new byte[] { 0xf0, 0xa4, 0xad, 0xa2 });
                Redis.Set("test1", bytes4);
                Assert.AreEqual("𤭢", Redis.Get("test1"));
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void TestRawBytes()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                double pi = Math.PI;
                byte[] bytes = BitConverter.GetBytes(pi);
                Redis.Set("test1", bytes);

                byte[] buffer = new byte[sizeof(double)];
                Redis.BufferFor(x => x.Get("test1"));
                Redis.Read(buffer, 0, buffer.Length);

                for (int i = 0; i < bytes.Length; i++)
                    Assert.AreEqual(bytes[i], buffer[i]);
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void TestAppend()
        {
            Redis.Del("test");
            var resp1 = Redis.Append("test", "Hello");
            Assert.AreEqual(5, resp1);

            var resp2 = Redis.Append("test", " World");
            Assert.AreEqual(11, resp2);

            var resp3 = Redis.Get("test");
            Assert.AreEqual("Hello World", resp3);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestBitCount()
        {
            Redis.Del("test");

            Redis.Set("test", "foobar");
            var resp1 = Redis.BitCount("test");
            Assert.AreEqual(26, resp1);

            var resp2 = Redis.BitCount("test", 0, 0);
            Assert.AreEqual(4, resp2);

            var resp3 = Redis.BitCount("test", 1, 1);
            Assert.AreEqual(6, resp3);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestBitOp()
        {
            Redis.Del("test1", "test2", "test3");

            Redis.Set("test1", "foobar");
            Redis.Set("test2", "abcdef");

            var resp1 = Redis.BitOp(RedisBitOp.And, "test3", "test1", "test2");
            Assert.AreEqual(6, resp1);

            Redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestBitPos()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Set("test1", new byte[] { 0xff, 0xf0, 0x00 });
                Assert.AreEqual(12, Redis.BitPos("test1", 0));

                /*Redis.Set("test1", "\x00\xff\xf0");
                Assert.AreEqual(8, Redis.BitPos("test1", 1, 0));
                Assert.AreEqual(8, Redis.BitPos("test1", 1, 1));

                Redis.Set("test1", "\x00\x00\x00");
                Assert.AreEqual(-1, Redis.BitPos("test1", 1));*/
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void TestDecr()
        {
            Redis.Del("test");

            Redis.Set("test", 10);
            var resp1 = Redis.Decr("test");
            Assert.AreEqual(9, resp1);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestDecrBy()
        {
            Redis.Del("test");

            Redis.Set("test", 10);
            var resp1 = Redis.DecrBy("test", 5);
            Assert.AreEqual(5, resp1);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGet()
        {
            Redis.Del("test");
            Assert.IsNull(Redis.Get("test"));

            Redis.Set("test", 1);
            Assert.AreEqual("1", Redis.Get("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetBit()
        {
            Redis.Del("test");

            Redis.SetBit("test", 7, true);
            Assert.IsFalse(Redis.GetBit("test", 0));
            Assert.IsTrue(Redis.GetBit("test", 7));
            Assert.IsFalse(Redis.GetBit("test", 100));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetRange()
        {
            Redis.Del("test");

            Redis.Set("test", "This is a string");
            Assert.AreEqual("This", Redis.GetRange("test", 0, 3));
            Assert.AreEqual("ing", Redis.GetRange("test", -3, -1));
            Assert.AreEqual("This is a string", Redis.GetRange("test", 0, -1));
            Assert.AreEqual("string", Redis.GetRange("test", 10, 100));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestGetSet()
        {
            Redis.Del("test");

            Redis.Set("test", "Hello");
            Assert.AreEqual("Hello", Redis.GetSet("test", "World"));
            Assert.AreEqual("World", Redis.Get("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncr()
        {
            Redis.Del("test");

            Redis.Set("test", 10);
            Assert.AreEqual(11, Redis.Incr("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncrBy()
        {
            Redis.Del("test");

            Redis.Set("test", 10);
            Assert.AreEqual(15, Redis.IncrBy("test", 5));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestIncrByFloat()
        {
            Redis.Del("test");

            Redis.Set("test", 10.50);
            Assert.AreEqual(10.6, Redis.IncrByFloat("test", 0.1));

            Redis.Set("test", "5.0e3");
            Assert.AreEqual(5200, Redis.IncrByFloat("test", 200));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMGet()
        {
            Redis.Del("test1", "test2", "test3");

            Redis.Set("test1", 1);
            Redis.Set("test2", 2);

            var resp1 = Redis.MGet("test1", "test2", "test3");
            Assert.AreEqual("1", resp1[0]);
            Assert.AreEqual("2", resp1[1]);
            Assert.IsNull(resp1[2]);

            Redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMSet()
        {
            // test array
            Redis.Del("test1", "test2", "test3");
            Assert.AreEqual("OK", Redis.MSet("test1", "v1", "test2", "v2", "test3", "v3"));
            Assert.AreEqual("v1", Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));
            Assert.AreEqual("v3", Redis.Get("test3"));

            // test kvp
            Redis.Del("test1", "test2", "test3");
            Assert.AreEqual("OK", Redis.MSet(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
                Tuple.Create("test3", "v3"),
            }));
            Assert.AreEqual("v1", Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));
            Assert.AreEqual("v3", Redis.Get("test3"));

            Redis.Del("test1", "test2", "test3");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestMSetNx()
        {
            // test array
            Redis.Del("test1", "test2");
            Assert.IsTrue(Redis.MSetNx("test1", "v1", "test2", "v2"));
            Assert.AreEqual("v1", Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));
            Redis.Del("test1");
            Assert.IsFalse(Redis.MSetNx("test1", "V1", "test2", "V2"));
            Assert.IsNull(Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));

            // test kvp
            Redis.Del("test1", "test2");
            Assert.IsTrue(Redis.MSetNx(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
            }));
            Assert.AreEqual("v1", Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));
            Redis.Del("test1");
            Assert.IsFalse(Redis.MSetNx(new[]
            {
                Tuple.Create("test1", "v1"),
                Tuple.Create("test2", "v2"),
            }));
            Assert.IsNull(Redis.Get("test1"));
            Assert.AreEqual("v2", Redis.Get("test2"));

            Redis.Del("test1", "test2");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestPSetEx()
        {
            Redis.Del("test");

            Assert.AreEqual("OK", Redis.PSetEx("test", 10000, 1));
            Assert.AreEqual("1", Redis.Get("test"));
            Assert.IsTrue(Redis.PTtl("test") > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSet()
        {
            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.Set("test", 1));
                Assert.AreEqual("1", Redis.Get("test"));
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.Set("test", 1, 10));
                var pttl = Redis.PTtl("test");
                Assert.IsTrue(pttl > 0);
                Assert.IsTrue(pttl <= 10000L);
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.Set("test", 1, 10000L));
                var ttl = Redis.Ttl("test");
                Assert.IsTrue(ttl > 0);
                Assert.IsTrue(ttl <= 10);
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.Set("test", 1, null, RedisExistence.Nx));
                Assert.IsNull(Redis.Set("test", 2, null, RedisExistence.Nx));
                Assert.AreEqual("1", Redis.Get("test"));
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.IsNull(Redis.Set("test", 1, null, RedisExistence.Xx));
                Assert.AreEqual("OK", Redis.Set("test", 2, null, RedisExistence.Nx));
                Assert.AreEqual("2", Redis.Get("test"));
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.Set("test", 1, TimeSpan.FromSeconds(10), RedisExistence.Nx));
                Assert.IsNull(Redis.Set("test", 2, null, RedisExistence.Nx));
                Assert.AreEqual("1", Redis.Get("test"));
                var pttl = Redis.PTtl("test");
                Assert.IsTrue(pttl > 0);
                Assert.IsTrue(pttl <= 10000L);
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetBit()
        {
            Redis.Del("test");

            Assert.IsFalse(Redis.SetBit("test", 7, true));
            Assert.IsTrue(Redis.SetBit("test", 7, false));
            Assert.AreEqual("\0", Redis.Get("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetEx()
        {
            Redis.Del("test");

            Assert.AreEqual("OK", Redis.SetEx("test", 10, 1));
            Assert.AreEqual("1", Redis.Get("test"));
            Assert.IsTrue(Redis.Ttl("test") > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetNx()
        {
            Redis.Del("test");

            Assert.IsTrue(Redis.SetNx("test", "Hello"));
            Assert.IsFalse(Redis.SetNx("test", "World"));
            Assert.AreEqual("Hello", Redis.Get("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestSetRange()
        {
            Redis.Del("test");

            Redis.Set("test", "Hello World");
            Assert.AreEqual(11, Redis.SetRange("test", 6, "Redis"));
            Assert.AreEqual("Hello Redis", Redis.Get("test"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Strings")]
        public void TestStrLen()
        {
            Redis.Del("test1", "test2");

            Redis.Set("test1", "Hello World");
            Assert.AreEqual(11, Redis.StrLen("test1"));
            Assert.AreEqual(0, Redis.StrLen("test2"));

            Redis.Del("test1", "test2");
        }
    }
}
