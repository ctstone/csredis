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
    public class SetTests
    {
        [TestMethod, TestCategory("Sets")]
        public void TestSAdd()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.SAdd("test", "test1"));
                Assert.AreEqual("*3\r\n$4\r\nSADD\r\n$4\r\ntest\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSCard()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.SCard("test"));
                Assert.AreEqual("*2\r\n$5\r\nSCARD\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiff()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.SDiff("test", "another");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*3\r\n$5\r\nSDIFF\r\n$4\r\ntest\r\n$7\r\nanother\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSDiffStore()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.SDiffStore("destination", "key1", "key2"));
                Assert.AreEqual("*4\r\n$10\r\nSDIFFSTORE\r\n$11\r\ndestination\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestInter()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.SInter("test", "another");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*3\r\n$6\r\nSINTER\r\n$4\r\ntest\r\n$7\r\nanother\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSInterStore()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.SInterStore("destination", "key1", "key2"));
                Assert.AreEqual("*4\r\n$11\r\nSINTERSTORE\r\n$11\r\ndestination\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSIsMember()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.SIsMember("test", "test1"));
                Assert.AreEqual("*3\r\n$9\r\nSISMEMBER\r\n$4\r\ntest\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMembers()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.SMembers("test");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*2\r\n$8\r\nSMEMBERS\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSMove()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.SMove("test", "destination", "test1"));
                Assert.AreEqual("*4\r\n$5\r\nSMOVE\r\n$4\r\ntest\r\n$11\r\ndestination\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSPop()
        {
            using (var mock = new FakeRedisSocket("$5\r\ntest1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("test1", redis.SPop("test"));
                Assert.AreEqual("*2\r\n$4\r\nSPOP\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRandMember()
        {
            using (var mock = new FakeRedisSocket("$5\r\ntest1\r\n", "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("test1", redis.SRandMember("test"));
                Assert.AreEqual("*2\r\n$11\r\nSRANDMEMBER\r\n$4\r\ntest\r\n", mock.GetMessage());

                var response = redis.SRandMember("test", 2);
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*3\r\n$11\r\nSRANDMEMBER\r\n$4\r\ntest\r\n$1\r\n2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSRem()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.SRem("test", "test1", "test2"));
                Assert.AreEqual("*4\r\n$4\r\nSREM\r\n$4\r\ntest\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSScan()
        {
            string reply = "*2\r\n$2\r\n23\r\n*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.SScan("test", 0);
                Assert.AreEqual(23, response1.Cursor);
                Assert.AreEqual(2, response1.Items.Length);
                Assert.AreEqual("test1", response1.Items[0]);
                Assert.AreEqual("test2", response1.Items[1]);
                Assert.AreEqual("*3\r\n$5\r\nSSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n", mock.GetMessage(), "Basic test");

                var response2 = redis.SScan("test", 0, pattern: "*");
                Assert.AreEqual("*5\r\n$5\r\nSSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n", mock.GetMessage(), "Pattern test");

                var response3 = redis.SScan("test", 0, count: 5);
                Assert.AreEqual("*5\r\n$5\r\nSSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Count test");

                var response4 = redis.SScan("test", 0, "*", 5);
                Assert.AreEqual("*7\r\n$5\r\nSSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Pattern + Count test");
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnion()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.SUnion("test", "another");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*3\r\n$6\r\nSUNION\r\n$4\r\ntest\r\n$7\r\nanother\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Sets")]
        public void TestSUnionStore()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.SUnionStore("destination", "key1", "key2"));
                Assert.AreEqual("*4\r\n$11\r\nSUNIONSTORE\r\n$11\r\ndestination\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());
            }
        }
    }
}
