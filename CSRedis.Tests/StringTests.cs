using CSRedis.Internal;
using CSRedis.Internal.Fakes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Tests
{
    [TestClass]
    public class StringTests
    {
        [TestMethod, TestCategory("Strings")]
        public void AppendTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.Append("key", "x"));
                Assert.AreEqual("*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$1\r\nx\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void BitCountTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n", ":4\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.BitCount("key"));
                Assert.AreEqual("*2\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n", mock.GetMessage());

                Assert.AreEqual(4, redis.BitCount("key", 0, 1));
                Assert.AreEqual("*4\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void BitOpTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n", ":10\r\n", ":10\r\n", ":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.BitOp(RedisBitOp.And, "dest", "key1", "key2"));
                Assert.AreEqual("*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$4\r\ndest\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());

                Assert.AreEqual(10, redis.BitOp(RedisBitOp.Or, "dest", "key1", "key2"));
                Assert.AreEqual("*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$4\r\ndest\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());

                Assert.AreEqual(10, redis.BitOp(RedisBitOp.XOr, "dest", "key1", "key2"));
                Assert.AreEqual("*5\r\n$5\r\nBITOP\r\n$3\r\nXOR\r\n$4\r\ndest\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());

                Assert.AreEqual(10, redis.BitOp(RedisBitOp.Not, "dest", "key1", "key2"));
                Assert.AreEqual("*5\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$4\r\ndest\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void BitPosTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n", ":10\r\n", ":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.BitPos("key", true));
                Assert.AreEqual("*3\r\n$6\r\nBITPOS\r\n$3\r\nkey\r\n$1\r\n1\r\n", mock.GetMessage());

                Assert.AreEqual(10, redis.BitPos("key", true, start: 0));
                Assert.AreEqual("*4\r\n$6\r\nBITPOS\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n0\r\n", mock.GetMessage());

                Assert.AreEqual(10, redis.BitPos("key", true, start: 0, end: 1));
                Assert.AreEqual("*5\r\n$6\r\nBITPOS\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n0\r\n$1\r\n1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void DecrTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.Decr("key"));
                Assert.AreEqual("*2\r\n$4\r\nDECR\r\n$3\r\nkey\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void DecrByTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.DecrBy("key", 5));
                Assert.AreEqual("*3\r\n$6\r\nDECRBY\r\n$3\r\nkey\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void GetTest()
        {
            using (var mock = new FakeRedisSocket("$5\r\nhello\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("hello", redis.Get("key"));
                Assert.AreEqual("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void GetBitTest()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.GetBit("key", 10));
                Assert.AreEqual("*3\r\n$6\r\nGETBIT\r\n$3\r\nkey\r\n$2\r\n10\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void GetRangeTest()
        {
            using (var mock = new FakeRedisSocket("$5\r\nhello\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("hello", redis.GetRange("key", 0, 10));
                Assert.AreEqual("*4\r\n$8\r\nGETRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n10\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void GetSetTest()
        {
            using (var mock = new FakeRedisSocket("$5\r\nhello\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("hello", redis.GetSet("key", "new"));
                Assert.AreEqual("*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$3\r\nnew\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void IncrTest()
        {
            using (var mock = new FakeRedisSocket(":5\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(5, redis.Incr("key"));
                Assert.AreEqual("*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void IncrByTest()
        {
            using (var mock = new FakeRedisSocket(":5\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(5, redis.IncrBy("key", 2));
                Assert.AreEqual("*3\r\n$6\r\nINCRBY\r\n$3\r\nkey\r\n$1\r\n2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void IncrByFloatTest()
        {
            using (var mock = new FakeRedisSocket("$4\r\n4.14\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(4.14, redis.IncrByFloat("key", 3.14));
                Assert.AreEqual("*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nkey\r\n$4\r\n3.14\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void MGetTest()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$4\r\nval1\r\n$4\r\nval2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.MGet("key1", "key2");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("val1", response[0]);
                Assert.AreEqual("val2", response[1]);
                Assert.AreEqual("*3\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void MSetTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n", "+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.MSet("key1", "val1", "key2", "val2"));
                Assert.AreEqual("*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.MSet(Tuple.Create("key1", "val1"), Tuple.Create("key2", "val2")));
                Assert.AreEqual("*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void MSetNxTest()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.MSetNx("key1", "val1", "key2", "val2"));
                Assert.AreEqual("*5\r\n$6\r\nMSETNX\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n", mock.GetMessage());

                Assert.IsFalse(redis.MSetNx(Tuple.Create("key1", "val1"), Tuple.Create("key2", "val2")));
                Assert.AreEqual("*5\r\n$6\r\nMSETNX\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void PSetExTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.PSetEx("key", 1000, "value"));
                Assert.AreEqual("*4\r\n$6\r\nPSETEX\r\n$3\r\nkey\r\n$4\r\n1000\r\n$5\r\nvalue\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void SetTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n", "+OK\r\n", "+OK\r\n", "+OK\r\n", "$-1\r\n", "$-1\r\n", "$-1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Set("key", "value"));
                Assert.AreEqual("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.Set("key", "value", expirationSeconds: 1));
                Assert.AreEqual("*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.Set("key", "value", expirationSeconds: 1, condition: RedisExistence.Nx));
                Assert.AreEqual("*6\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n$2\r\nNX\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.Set("key", "value", expirationSeconds: 1, condition: RedisExistence.Xx));
                Assert.AreEqual("*6\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n$2\r\nXX\r\n", mock.GetMessage());

                Assert.IsNull(redis.Set("key", "value", expirationMilliseconds: 1));
                Assert.AreEqual("*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n1\r\n", mock.GetMessage());

                Assert.IsNull(redis.Set("key", "value", expirationMilliseconds: 1, condition: RedisExistence.Nx));
                Assert.AreEqual("*6\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n1\r\n$2\r\nNX\r\n", mock.GetMessage());

                Assert.IsNull(redis.Set("key", "value", expirationMilliseconds: 1, condition: RedisExistence.Xx));
                Assert.AreEqual("*6\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n1\r\n$2\r\nXX\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void SetBitTest()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.SetBit("key", 5, true));
                Assert.AreEqual("*4\r\n$6\r\nSETBIT\r\n$3\r\nkey\r\n$1\r\n5\r\n$1\r\n1\r\n", mock.GetMessage());

                Assert.IsFalse(redis.SetBit("key", 5, false));
                Assert.AreEqual("*4\r\n$6\r\nSETBIT\r\n$3\r\nkey\r\n$1\r\n5\r\n$1\r\n0\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void SetExTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.SetEx("key", 100, "value"));
                Assert.AreEqual("*4\r\n$5\r\nSETEX\r\n$3\r\nkey\r\n$3\r\n100\r\n$5\r\nvalue\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void SetNxTest()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.SetNx("key", "value"));
                Assert.AreEqual("*3\r\n$5\r\nSETNX\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void SetRangeTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.SetRange("key", 4, "value"));
                Assert.AreEqual("*4\r\n$8\r\nSETRANGE\r\n$3\r\nkey\r\n$1\r\n4\r\n$5\r\nvalue\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Strings")]
        public void StrLenTest()
        {
            using (var mock = new FakeRedisSocket(":10\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(10, redis.StrLen("key"));
                Assert.AreEqual("*2\r\n$6\r\nSTRLEN\r\n$3\r\nkey\r\n", mock.GetMessage());
            }
        }
    }
}
