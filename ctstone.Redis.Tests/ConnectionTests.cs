using ctstone.Redis.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ctstone.Redis.Tests
{
    [TestClass]
    public class ConnectionTests
    {
        [TestMethod, TestCategory("Connection")]
        public void AuthTest()
        {
            using (var mock = new MockConnector("MockHost", 9999, "+OK\r\n", "+OK\r\n"))
            using (var redis = new RedisClient(mock, new UTF8Encoding(false)))
            {
                Assert.AreEqual("OK", redis.Auth("my password"));
                Assert.AreEqual("*2\r\n$4\r\nAUTH\r\n$11\r\nmy password\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.AuthAsync("my password").Result);
                Assert.AreEqual("*2\r\n$4\r\nAUTH\r\n$11\r\nmy password\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Connection")]
        public void EchoTest()
        {
            using (var mock = new MockConnector("MockHost", 9999, "$11\r\nhello world\r\n"))
            using (var redis = new RedisClient(mock, new UTF8Encoding(false)))
            {
                Assert.AreEqual("hello world", redis.Echo("hello world"));
                Assert.AreEqual("*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Connection")]
        public void PingTest()
        {
            using (var mock = new MockConnector("MockHost", 9999, "+PONG\r\n"))
            using (var redis = new RedisClient(mock, new UTF8Encoding(false)))
            {
                Assert.AreEqual("PONG", redis.Ping());
                Assert.AreEqual("*1\r\n$4\r\nPING\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Connection")]
        public void QuitTest()
        {
            using (var mock = new MockConnector("MockHost", 9999, "+OK\r\n"))
            using (var redis = new RedisClient(mock, new UTF8Encoding(false)))
            {
                Assert.AreEqual("OK", redis.Quit());
                Assert.AreEqual("*1\r\n$4\r\nQUIT\r\n", mock.GetMessage());
                Assert.IsFalse(redis.Connected);
            }
        }

        [TestMethod, TestCategory("Connection")]
        public void SelectTest()
        {
            using (var mock = new MockConnector("MockHost", 9999, "+OK\r\n"))
            using (var redis = new RedisClient(mock, new UTF8Encoding(false)))
            {
                Assert.AreEqual("OK", redis.Select(2));
                Assert.AreEqual("*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n", mock.GetMessage());
            }
        }
    }
}
