using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisScriptingTests : RedisTestBase
    {
        [TestMethod, TestCategory("Scripting")]
        public void TestEval()
        {
            var resp = (object[])_redis.Eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", new[] { "key1", "key2" }, "first", "second");
            Assert.AreEqual(4, resp.Length);
            Assert.AreEqual("key1", resp[0]);
            Assert.AreEqual("key2", resp[1]);
            Assert.AreEqual("first", resp[2]);
            Assert.AreEqual("second", resp[3]);
        }

        [TestMethod, TestCategory("Scripting")]
        public void TestScriptLoad()
        {
            Assert.AreEqual("a42059b356c875f0717db19a51f6aaca9ae659ea", _redis.ScriptLoad("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"));
        }

        [TestMethod, TestCategory("Scripting")]
        public void TestEvalSHA()
        {
            var sha1 = _redis.ScriptLoad("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}");
            var resp = (object[])_redis.EvalSHA(sha1, new[] { "key1", "key2" }, "first", "second");
            Assert.AreEqual(4, resp.Length);
            Assert.AreEqual("key1", resp[0]);
            Assert.AreEqual("key2", resp[1]);
            Assert.AreEqual("first", resp[2]);
            Assert.AreEqual("second", resp[3]);
        }

        [TestMethod, TestCategory("Scripting")]
        public void TestScriptExists()
        {
            var sha1 = _redis.ScriptLoad("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}");
            var resp = _redis.ScriptExists(sha1, "junk");
            Assert.IsTrue(resp[0]);
            Assert.IsFalse(resp[1]);
        }

        [TestMethod, TestCategory("Scripting")]
        public void TestScriptFlush()
        {
            var sha1 = _redis.ScriptLoad("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}");
            Assert.IsTrue(_redis.ScriptExists(sha1)[0]);
            Assert.AreEqual("OK", _redis.ScriptFlush());
            Assert.IsFalse(_redis.ScriptExists(sha1)[0]);
        }

        [TestMethod, TestCategory("Scripting")]
        public void TestScriptKill()
        {
            try
            {
                Assert.AreEqual("OK", _redis.ScriptKill());
            }
            catch (Exception e)
            {
                Assert.IsTrue(e.Message.StartsWith("NOTBUSY"));
            }
        }
    }
}
