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
    public class KeyTests
    {
        [TestMethod, TestCategory("Keys")]
        public void TestDel()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.Del("test"));
                Assert.AreEqual("*2\r\n$3\r\nDEL\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestDump()
        {
            using (var mock = new FakeRedisSocket("$4\r\ntest\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("test", redis.Encoding.GetString(redis.Dump("test")));
                Assert.AreEqual("*2\r\n$4\r\nDUMP\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExists()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.Exists("test1"));
                Assert.AreEqual("*2\r\n$6\r\nEXISTS\r\n$5\r\ntest1\r\n", mock.GetMessage());
                Assert.IsFalse(redis.Exists("test2"));
                Assert.AreEqual("*2\r\n$6\r\nEXISTS\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpire()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.Expire("test1", TimeSpan.FromSeconds(10)));
                Assert.AreEqual("*3\r\n$6\r\nEXPIRE\r\n$5\r\ntest1\r\n$2\r\n10\r\n", mock.GetMessage());
                Assert.IsFalse(redis.Expire("test2", 20));
                Assert.AreEqual("*3\r\n$6\r\nEXPIRE\r\n$5\r\ntest2\r\n$2\r\n20\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpireAt()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                // 1402643208 = ISO 8601:2014-06-13T07:06:48Z
                Assert.IsTrue(redis.ExpireAt("test1", new DateTime(2014, 6, 13, 7, 6, 48)));
                Assert.AreEqual("*3\r\n$8\r\nEXPIREAT\r\n$5\r\ntest1\r\n$10\r\n1402643208\r\n", mock.GetMessage());
                Assert.IsFalse(redis.ExpireAt("test2", 1402643208));
                Assert.AreEqual("*3\r\n$8\r\nEXPIREAT\r\n$5\r\ntest2\r\n$10\r\n1402643208\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestKeys()
        {
            using (var mock = new FakeRedisSocket("*3\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n$5\r\ntest3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.Keys("test*");
                Assert.AreEqual(3, response.Length);
                for (int i = 0; i < response.Length; i++)
                    Assert.AreEqual("test" + (i + 1), response[i]);
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestMigrate()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n", "+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Migrate("myhost", 1234, "mykey", 3, 1000));
                Assert.AreEqual("*6\r\n$7\r\nMIGRATE\r\n$6\r\nmyhost\r\n$4\r\n1234\r\n$5\r\nmykey\r\n$1\r\n3\r\n$4\r\n1000\r\n", mock.GetMessage());

                Assert.AreEqual("OK", redis.Migrate("myhost2", 1235, "mykey2", 6, TimeSpan.FromMilliseconds(100)));
                Assert.AreEqual("*6\r\n$7\r\nMIGRATE\r\n$7\r\nmyhost2\r\n$4\r\n1235\r\n$6\r\nmykey2\r\n$1\r\n6\r\n$3\r\n100\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestMove()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.Move("test", 5));
                Assert.AreEqual("*3\r\n$4\r\nMOVE\r\n$4\r\ntest\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestObjectEncoding()
        {
            using (var mock = new FakeRedisSocket("$5\r\ntest1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("test1", redis.ObjectEncoding("test"));
                Assert.AreEqual("*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestObject()
        {
            using (var mock = new FakeRedisSocket(":5555\r\n", ":9999\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(5555, redis.Object(RedisObjectSubCommand.RefCount, "test1"));
                Assert.AreEqual("*3\r\n$6\r\nOBJECT\r\n$8\r\nREFCOUNT\r\n$5\r\ntest1\r\n", mock.GetMessage());

                Assert.AreEqual(9999, redis.Object(RedisObjectSubCommand.IdleTime, "test2"));
                Assert.AreEqual("*3\r\n$6\r\nOBJECT\r\n$8\r\nIDLETIME\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void Persist()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.Persist("test1"));
                Assert.AreEqual("*2\r\n$7\r\nPERSIST\r\n$5\r\ntest1\r\n", mock.GetMessage());
                Assert.IsFalse(redis.Persist("test2"));
                Assert.AreEqual("*2\r\n$7\r\nPERSIST\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void PExpire()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.PExpire("test1", TimeSpan.FromMilliseconds(5000)));
                Assert.AreEqual("*3\r\n$7\r\nPEXPIRE\r\n$5\r\ntest1\r\n$4\r\n5000\r\n", mock.GetMessage());
                Assert.IsFalse(redis.PExpire("test2", 6000));
                Assert.AreEqual("*3\r\n$7\r\nPEXPIRE\r\n$5\r\ntest2\r\n$4\r\n6000\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void PExpireAt()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                // 1402643208123 = ISO 8601:2014-06-13T07:06:48Z +123ms

                Assert.IsTrue(redis.PExpireAt("test1", new DateTime(2014, 6, 13, 7, 6, 48, 123)));
                Assert.AreEqual("*3\r\n$9\r\nPEXPIREAT\r\n$5\r\ntest1\r\n$13\r\n1402643208123\r\n", mock.GetMessage());
                Assert.IsFalse(redis.PExpireAt("test2", 1402643208123));
                Assert.AreEqual("*3\r\n$9\r\nPEXPIREAT\r\n$5\r\ntest2\r\n$13\r\n1402643208123\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestPttl()
        {
            using (var mock = new FakeRedisSocket(":123\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(123, redis.PTtl("test"));
                Assert.AreEqual("*2\r\n$4\r\nPTTL\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRandomKey()
        {
            using (var mock = new FakeRedisSocket("$7\r\nsomekey\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("somekey", redis.RandomKey());
                Assert.AreEqual("*1\r\n$9\r\nRANDOMKEY\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRename()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Rename("test1", "test2"));
                Assert.AreEqual("*3\r\n$6\r\nRENAME\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRenameNx()
        {
            using (var mock = new FakeRedisSocket(":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.IsTrue(redis.RenameNx("test1", "test2"));
                Assert.AreEqual("*3\r\n$8\r\nRENAMENX\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRestore()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Restore("test", 123, "abc"));
                Assert.AreEqual("*4\r\n$7\r\nRESTORE\r\n$4\r\ntest\r\n$3\r\n123\r\n$3\r\nabc\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestSort()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$2\r\nab\r\n$2\r\ncd\r\n", "*0\r\n", "*0\r\n", "*0\r\n", "*0\r\n", "*0\r\n", "*0\r\n", "*0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var resp1 = redis.Sort("test1");
                Assert.AreEqual(2, resp1.Length);
                Assert.AreEqual("ab", resp1[0]);
                Assert.AreEqual("cd", resp1[1]);
                Assert.AreEqual("*2\r\n$4\r\nSORT\r\n$5\r\ntest1\r\n", mock.GetMessage());

                var resp2 = redis.Sort("test2", offset: 0, count: 2);
                Assert.AreEqual("*5\r\n$4\r\nSORT\r\n$5\r\ntest2\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$1\r\n2\r\n", mock.GetMessage());

                var resp3 = redis.Sort("test3", by: "xyz");
                Assert.AreEqual("*4\r\n$4\r\nSORT\r\n$5\r\ntest3\r\n$2\r\nBY\r\n$3\r\nxyz\r\n", mock.GetMessage());

                var resp4 = redis.Sort("test4", dir: RedisSortDir.Asc);
                Assert.AreEqual("*3\r\n$4\r\nSORT\r\n$5\r\ntest4\r\n$3\r\nASC\r\n", mock.GetMessage());

                var resp5 = redis.Sort("test5", dir: RedisSortDir.Desc);
                Assert.AreEqual("*3\r\n$4\r\nSORT\r\n$5\r\ntest5\r\n$4\r\nDESC\r\n", mock.GetMessage());

                var resp6 = redis.Sort("test6", isAlpha: true);
                Assert.AreEqual("*3\r\n$4\r\nSORT\r\n$5\r\ntest6\r\n$5\r\nALPHA\r\n", mock.GetMessage());

                var resp7 = redis.Sort("test7", get: new[] { "get1", "get2" });
                Assert.AreEqual("*6\r\n$4\r\nSORT\r\n$5\r\ntest7\r\n$3\r\nGET\r\n$4\r\nget1\r\n$3\r\nGET\r\n$4\r\nget2\r\n", mock.GetMessage());

                var resp8 = redis.Sort("test8", offset: 0, count: 2, by: "xyz", dir: RedisSortDir.Asc, isAlpha: true, get: new[] { "a", "b" });
                Assert.AreEqual("*13\r\n$4\r\nSORT\r\n$5\r\ntest8\r\n$2\r\nBY\r\n$3\r\nxyz\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$1\r\n2\r\n$3\r\nGET\r\n$1\r\na\r\n$3\r\nGET\r\n$1\r\nb\r\n$3\r\nASC\r\n$5\r\nALPHA\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestSortAndStore()
        {
            using (var mock = new FakeRedisSocket(":1\r\n", ":1\r\n", ":1\r\n", ":1\r\n", ":1\r\n", ":1\r\n", ":1\r\n", ":1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(1, redis.SortAndStore("test1", "test2"));
                Assert.AreEqual("*4\r\n$4\r\nSORT\r\n$5\r\ntest1\r\n$5\r\nSTORE\r\n$5\r\ntest2\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test2", "test3", offset:0, count:2));
                Assert.AreEqual("*7\r\n$4\r\nSORT\r\n$5\r\ntest2\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$1\r\n2\r\n$5\r\nSTORE\r\n$5\r\ntest3\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test3", "test4", by: "xyz"));
                Assert.AreEqual("*6\r\n$4\r\nSORT\r\n$5\r\ntest3\r\n$2\r\nBY\r\n$3\r\nxyz\r\n$5\r\nSTORE\r\n$5\r\ntest4\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test5", "test6", dir: RedisSortDir.Asc));
                Assert.AreEqual("*5\r\n$4\r\nSORT\r\n$5\r\ntest5\r\n$3\r\nASC\r\n$5\r\nSTORE\r\n$5\r\ntest6\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test7", "test8", dir: RedisSortDir.Desc));
                Assert.AreEqual("*5\r\n$4\r\nSORT\r\n$5\r\ntest7\r\n$4\r\nDESC\r\n$5\r\nSTORE\r\n$5\r\ntest8\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test9", "test10", isAlpha: true));
                Assert.AreEqual("*5\r\n$4\r\nSORT\r\n$5\r\ntest9\r\n$5\r\nALPHA\r\n$5\r\nSTORE\r\n$6\r\ntest10\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test11", "test12", get: new[] { "get1", "get2" }));
                Assert.AreEqual("*8\r\n$4\r\nSORT\r\n$6\r\ntest11\r\n$3\r\nGET\r\n$4\r\nget1\r\n$3\r\nGET\r\n$4\r\nget2\r\n$5\r\nSTORE\r\n$6\r\ntest12\r\n", mock.GetMessage());

                Assert.AreEqual(1, redis.SortAndStore("test13", "test14", offset: 0, count: 2, by: "xyz", dir: RedisSortDir.Asc, isAlpha: true, get: new[] { "a", "b" }));
                Assert.AreEqual("*15\r\n$4\r\nSORT\r\n$6\r\ntest13\r\n$2\r\nBY\r\n$3\r\nxyz\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$1\r\n2\r\n$3\r\nGET\r\n$1\r\na\r\n$3\r\nGET\r\n$1\r\nb\r\n$3\r\nASC\r\n$5\r\nALPHA\r\n$5\r\nSTORE\r\n$6\r\ntest14\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestTtl()
        {
            using (var mock = new FakeRedisSocket(":123\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(123, redis.Ttl("test"));
                Assert.AreEqual("*2\r\n$3\r\nTTL\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestType()
        {
            using (var mock = new FakeRedisSocket("+OK\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual("OK", redis.Type("test"));
                Assert.AreEqual("*2\r\n$4\r\nTYPE\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("Keys")]
        public void TestScan()
        {
            var reply1 = "*2\r\n$1\r\n0\r\n*3\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n$5\r\ntest3\r\n";
            var reply2 = "*2\r\n$1\r\n0\r\n*0\r\n";
            var reply3 = "*2\r\n$1\r\n0\r\n*0\r\n";
            var reply4 = "*2\r\n$1\r\n0\r\n*0\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply2, reply3, reply4))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var resp = redis.Scan(0);
                Assert.AreEqual(0, resp.Cursor);
                Assert.AreEqual(3, resp.Items.Length);
                for (int i = 0; i < resp.Items.Length; i++)
                    Assert.AreEqual("test" + (i + 1), resp.Items[i]);
                Assert.AreEqual("*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n", mock.GetMessage());

                redis.Scan(1, pattern: "pattern");
                Assert.AreEqual("*4\r\n$4\r\nSCAN\r\n$1\r\n1\r\n$5\r\nMATCH\r\n$7\r\npattern\r\n", mock.GetMessage());

                redis.Scan(2, count: 5);
                Assert.AreEqual("*4\r\n$4\r\nSCAN\r\n$1\r\n2\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage());

                redis.Scan(3, pattern: "pattern", count: 5);
                Assert.AreEqual("*6\r\n$4\r\nSCAN\r\n$1\r\n3\r\n$5\r\nMATCH\r\n$7\r\npattern\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }
    }
}
