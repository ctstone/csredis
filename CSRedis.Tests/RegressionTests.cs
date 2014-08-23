using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CSRedis.Internal;

namespace CSRedis.Tests
{
    [TestClass]
    public class RegressionTests
    {
        [TestMethod, TestCategory("Regression")]
        public void SetUTF8Test()
        {
            using (var mock = new MockConnector("MockHost", 9999, "+OK\r\n", "+OK\r\n"))
            using (var redis = new RedisClient(mock))
            {
                Assert.AreEqual("OK", redis.Set("test", "é"));
                Assert.AreEqual("*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$2\r\né\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.SetAsync("test", "é").Result);
                Assert.AreEqual("*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$2\r\né\r\n", mock.GetMessage());
            }
        }
    }
}
