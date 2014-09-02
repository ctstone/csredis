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
    public class ScriptingTests
    {


        [TestMethod, TestCategory("Scripting")]
        public void EvalTest()
        {
            using (var mock = new FakeRedisSocket("*4\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.Eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", new[] { "key1", "key2" }, "first", "second");
                Assert.IsTrue(response is object[]);
                Assert.AreEqual(4, (response as object[]).Length);
                Assert.AreEqual("key1", (response as object[])[0]);
                Assert.AreEqual("key2", (response as object[])[1]);
                Assert.AreEqual("first", (response as object[])[2]);
                Assert.AreEqual("second", (response as object[])[3]);
                Assert.AreEqual("*7\r\n$4\r\nEVAL\r\n$40\r\nreturn {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Scripting")]
        public void EvalSHATest()
        {
            using (var mock = new FakeRedisSocket("*4\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.EvalSHA("checksum", new[] { "key1", "key2" }, "first", "second");
                Assert.IsTrue(response is object[]);
                Assert.AreEqual(4, (response as object[]).Length);
                Assert.AreEqual("key1", (response as object[])[0]);
                Assert.AreEqual("key2", (response as object[])[1]);
                Assert.AreEqual("first", (response as object[])[2]);
                Assert.AreEqual("second", (response as object[])[3]);
                Assert.AreEqual("*7\r\n$7\r\nEVALSHA\r\n$8\r\nchecksum\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Scripting")]
        public void ScriptExistsTests()
        {
            using (var mock = new FakeRedisSocket("*2\r\n:1\r\n:0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.ScriptExists("checksum1", "checksum2");
                Assert.AreEqual(2, response.Length);
                Assert.IsTrue(response[0]);
                Assert.IsFalse(response[1]);

                Assert.AreEqual("*4\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$9\r\nchecksum1\r\n$9\r\nchecksum2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Scripting")]
        public void ScriptFlushTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.ScriptFlush());
                Assert.AreEqual("*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Scripting")]
        public void ScriptKillTest()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.ScriptKill());
                Assert.AreEqual("*2\r\n$6\r\nSCRIPT\r\n$4\r\nKILL\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Scripting")]
        public void ScriptLoadTest()
        {
            using (var mock = new FakeRedisSocket("$8\r\nchecksum\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("checksum", redis.ScriptLoad("return 1"));
                Assert.AreEqual("*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n", mock.GetMessage());
            }
        }
    }
}
