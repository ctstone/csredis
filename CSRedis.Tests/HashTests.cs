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
    public class MyGeneric
    {
        public string field1 { get; set; }
    }

    [TestClass]
    public class HashTests
    {
        [TestMethod, TestCategory("Hashes")]
        public void TestHDel()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.HDel("test", "test1", "test2"));
                Assert.AreEqual("*4\r\n$4\r\nHDEL\r\n$4\r\ntest\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHExists()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.HExists("test", "field"));
                Assert.AreEqual("*3\r\n$7\r\nHEXISTS\r\n$4\r\ntest\r\n$5\r\nfield\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHGet()
        {
            using (var mock = new FakeRedisSocket("$4\r\ntest\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("test", redis.HGet("test", "field"));
                Assert.AreEqual("*3\r\n$4\r\nHGET\r\n$4\r\ntest\r\n$5\r\nfield\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHGetAll_Dictionary()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.HGetAll("test");
                Assert.AreEqual(1, response.Count);
                Assert.IsTrue(response.ContainsKey("field1"));
                Assert.AreEqual("test1", response["field1"]);
                Assert.AreEqual("*2\r\n$7\r\nHGETALL\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHGetAll_Generic()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.HGetAll<MyGeneric>("test");
                Assert.AreEqual("test1", response.field1);
                Assert.AreEqual("*2\r\n$7\r\nHGETALL\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHIncrBy()
        {
            using (var mock = new FakeRedisSocket(":5\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(5, redis.HIncrBy("test", "field", 1));
                Assert.AreEqual("*4\r\n$7\r\nHINCRBY\r\n$4\r\ntest\r\n$5\r\nfield\r\n$1\r\n1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHIncrByFloat()
        {
            using (var mock = new FakeRedisSocket("$4\r\n3.14\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3.14, redis.HIncrByFloat("test", "field", 1.14));
                Assert.AreEqual("*4\r\n$12\r\nHINCRBYFLOAT\r\n$4\r\ntest\r\n$5\r\nfield\r\n$4\r\n1.14\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHKeys()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.HKeys("test");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*2\r\n$5\r\nHKEYS\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHLen()
        {
            using (var mock = new FakeRedisSocket(":5\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(5, redis.HLen("test"));
                Assert.AreEqual("*2\r\n$4\r\nHLEN\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHMGet()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.HMGet("test", "field1", "field2");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*4\r\n$5\r\nHMGET\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHMSet_Array()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.HMSet("test", "field1", "test1"));
                Assert.AreEqual("*4\r\n$5\r\nHMSET\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHMSet_Generic()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.HMSet("test", new { field1 = "test1" }));
                Assert.AreEqual("*4\r\n$5\r\nHMSET\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHMSet_Dictionary()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.HMSet("test", new Dictionary<string, string> { { "field1", "test1" } }));
                Assert.AreEqual("*4\r\n$5\r\nHMSET\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHSet()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.HSet("test", "field1", "test1"));
                Assert.AreEqual("*4\r\n$4\r\nHSET\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHSetNX()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.HSetNx("test", "field1", "test1"));
                Assert.AreEqual("*4\r\n$6\r\nHSETNX\r\n$4\r\ntest\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHVals()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.HVals("test");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*2\r\n$5\r\nHVALS\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Hashes")]
        public void TestHScan()
        {
            string reply = "*2\r\n$2\r\n23\r\n*2\r\n$6\r\nfield1\r\n$5\r\ntest1\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.HScan("test", 0);
                Assert.AreEqual(23, response1.Cursor);
                Assert.AreEqual(1, response1.Items.Length);
                Assert.AreEqual("field1", response1.Items[0].Item1);
                Assert.AreEqual("test1", response1.Items[0].Item2);
                Assert.AreEqual("*3\r\n$5\r\nHSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n", mock.GetMessage(), "Basic test");

                var response2 = redis.HScan("test", 0, pattern: "*");
                Assert.AreEqual("*5\r\n$5\r\nHSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n", mock.GetMessage(), "Pattern test");

                var response3 = redis.HScan("test", 0, count: 5);
                Assert.AreEqual("*5\r\n$5\r\nHSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Count test");

                var response4 = redis.HScan("test", 0, "*", 5);
                Assert.AreEqual("*7\r\n$5\r\nHSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Pattern + Count test");
            }
        }
    }
}
