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
    public class TransactionTests
    {
        [TestMethod, TestCategory("Transactions")]
        public void DiscardTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Discard());
                Assert.AreEqual("*1\r\n$7\r\nDISCARD\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Transactions")]
        public void ExecTest()
        {
            using (var mock = new FakeRedisSocket("*1\r\n$2\r\nhi\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.Exec();
                Assert.AreEqual(1, response.Length);
                Assert.AreEqual("hi", response[0]);
                Assert.AreEqual("*1\r\n$4\r\nEXEC\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Transactions")]
        public void MultiTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Multi());
                Assert.AreEqual("*1\r\n$5\r\nMULTI\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Transactions")]
        public void UnwatchTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Unwatch());
                Assert.AreEqual("*1\r\n$7\r\nUNWATCH\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Transactions")]
        public void WatchTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Watch());
                Assert.AreEqual("*1\r\n$5\r\nWATCH\r\n", mock.GetMessage());
            }
        }
    }
}
