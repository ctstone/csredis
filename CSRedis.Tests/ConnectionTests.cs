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
    class TestHelper
    {
        public const string Host = "fakehost";
        public const int Port = 9999;

        public static void Test<T>(string reply, Func<IRedisClientSync, T> syncFunc, Func<IRedisClientAsync, Task<T>> asyncFunc, Action<FakeRedisSocket, T> test)
        {
            using (var mock = new FakeRedisSocket(reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint(Host, Port)))
            {
                if (syncFunc != null)
                {
                    var r1 = syncFunc(redis);
                    test(mock, r1);
                }

                if (asyncFunc != null)
                {
                    var r2 = asyncFunc(redis);
                    test(mock, r2.Result);
                }
            }
        }
    }


    [TestClass]
    public class ConnectionTests
    {
        [TestMethod, TestCategory("Connection")]
        public void AuthTest()
        {
            TestHelper.Test(
                "+OK\r\n",
                x => x.Auth("my password"),
                x => x.AuthAsync("my password"),
                (x, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual("*2\r\n$4\r\nAUTH\r\n$11\r\nmy password\r\n", x.GetMessage());
                });
        }

        [TestMethod, TestCategory("Connection")]
        public void EchoTest()
        {
            TestHelper.Test(
                "$11\r\nhello world\r\n",
                x => x.Echo("hello world"),
                x => x.EchoAsync("hello world"),
                (x, r) =>
                {
                    Assert.AreEqual("hello world", r);
                    Assert.AreEqual("*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n", x.GetMessage());
                });
        }

        [TestMethod, TestCategory("Connection")]
        public void PingTest()
        {
            TestHelper.Test(
                "+PONG\r\n",
                x => x.Ping(),
                x => x.PingAsync(),
                (x, r) =>
                {
                    Assert.AreEqual("PONG", r);
                    Assert.AreEqual("*1\r\n$4\r\nPING\r\n", x.GetMessage());
                });
        }

        [TestMethod, TestCategory("Connection")]
        public void QuitTest()
        {
            TestHelper.Test(
                "+OK\r\n",
                x => x.Quit(),
                null,
                (x, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual("*1\r\n$4\r\nQUIT\r\n", x.GetMessage());
                });

            TestHelper.Test(
                "+OK\r\n",
                null,
                x => x.QuitAsync(),
                (x, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual("*1\r\n$4\r\nQUIT\r\n", x.GetMessage());
                });
            // TODO: test Connected==false
        }

        [TestMethod, TestCategory("Connection")]
        public void SelectTest()
        {
            TestHelper.Test(
                "+OK\r\n",
                x => x.Select(2),
                x => x.SelectAsync(2),
                (x, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual("*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n", x.GetMessage());
                });
        }
    }
}
