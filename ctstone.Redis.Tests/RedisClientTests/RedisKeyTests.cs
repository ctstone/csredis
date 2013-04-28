using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisKeyTests : RedisTestBase
    {
        [TestMethod, TestCategory("Keys")]
        public void TestDel()
        {
            Redis.Del("test", "test1", "test2", "test3", "test4", "test5");
            Redis.MSet(new[] 
            {
                Tuple.Create("test", "t"),
                Tuple.Create("test1", "t1"),
                Tuple.Create("test2", "t2"),
                Tuple.Create("test3", "t3"),
                Tuple.Create("test4", "t4"),
            });

            var del1 = Redis.Del("test", "test1", "test2");
            Assert.AreEqual(3, del1);

            var del2 = Redis.Del("test3", "test4", "test5");
            Assert.AreEqual(2, del2);
        }

        [TestMethod, TestCategory("Keys")]
        public void TestDump()
        {
            Redis.Del("test");
            Redis.Set("test", 10);
            var res = Redis.Dump("test");
            byte[] expected = new byte[] 
            {
                0x0, 0xC0, 0xA, 0x06, 0x0, 0xF8, 0x72, 0x3F, 0xC5, 0xFB, 0xFB, 0x5F, 0x28
            };

            for (int i = 0; i < res.Length; i++)
                Assert.AreEqual(expected[i], res[i]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExists()
        {
            Redis.Del("test");

            var res1 = Redis.Exists("test");
            Assert.IsFalse(res1);

            Redis.Set("test", 1);
            
            var res2 = Redis.Exists("test");
            Assert.IsTrue(res2);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpire()
        {
            Redis.Del("test");

            var res1 = Redis.Expire("test", 10);
            Assert.IsFalse(res1);

            Redis.Set("test", "t1");
            var res2 = Redis.Expire("test", 10);
            Assert.IsTrue(res2);

            var res3 = Redis.Ttl("test");
            Assert.IsTrue(res3 > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpireAt()
        {
            Redis.Del("test");

            var server_time = Redis.Time();

            var res1 = Redis.ExpireAt("test", server_time + TimeSpan.FromSeconds(10));
            Assert.IsFalse(res1);

            Redis.Set("test", 1);

            var res2 = Redis.ExpireAt("test", server_time + TimeSpan.FromSeconds(10));
            Assert.IsTrue(res2);

            var res3 = Redis.Ttl("test");
            Assert.IsTrue(res3 > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestKeys()
        {
            string prefix = Guid.NewGuid().ToString();
            string[] keys = new[] { "test:" + prefix + ":1", "test:" + prefix + ":2", "test:" + prefix + ":3" };
            using (new RedisTestKeys(Redis, keys))
            {
                Redis.Del(keys);
                foreach (var key in keys)
                    Redis.Set(key, 1);

                Assert.AreEqual(keys.Length, Redis.Keys("test:" + prefix + ":*").Length);
            }
        }

        // MIGRATE

        // MOVE

        // OBJECT

        [TestMethod, TestCategory("Keys")]
        public void TestPersist()
        {
            Redis.Del("test");
            
            Redis.Set("test", "t1");
            Redis.Expire("test", 10);

            var res1 = Redis.Ttl("test");
            Assert.IsTrue(res1 > 0);

            var res2 = Redis.Persist("test");
            Assert.IsTrue(res2);

            var res3 = Redis.Ttl("test");
            Assert.AreEqual(-1, res3);

            Redis.Del("test");

            var res4 = Redis.Persist("test");
            Assert.IsFalse(res4);
        }

        [TestMethod, TestCategory("Keys")]
        public void TestPExpire()
        {
            Redis.Del("test");

            var res1 = Redis.PExpire("test", 10000);
            Assert.IsFalse(res1);

            Redis.Set("test", "t1");
            var res2 = Redis.PExpire("test", 10000);
            Assert.IsTrue(res2);

            var res3 = Redis.PTtl("test");
            Assert.IsTrue(res3 > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestPExpireAt()
        {
            Redis.Del("test");

            var server_time = Redis.Time();

            var res1 = Redis.PExpireAt("test", server_time + TimeSpan.FromMilliseconds(10000));
            Assert.IsFalse(res1);

            Redis.Set("test", 1);

            var res2 = Redis.PExpireAt("test", server_time + TimeSpan.FromMilliseconds(10000));
            Assert.IsTrue(res2);

            var res3 = Redis.PTtl("test");
            Assert.IsTrue(res3 > 0);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRandomKey()
        {
            Redis.Del("test");

            Redis.Set("test", 1);
            var res = Redis.RandomKey();
            Assert.IsNotNull(res);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRename()
        {
            Redis.Del("test", "test2");

            string guid = Guid.NewGuid().ToString();
            Redis.Set("test", guid);
            var resp1 = Redis.Rename("test", "test2");
            Assert.AreEqual("OK", resp1);

            var resp2 = Redis.Exists("test");
            Assert.IsFalse(resp2);

            var resp3 = Redis.Get("test2");
            Assert.AreEqual(guid, resp3);

            Redis.Del("test", "test2");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRenameX()
        {
            Redis.Del("test", "test2", "test3");

            string guid = Guid.NewGuid().ToString();
            Redis.Set("test", guid);
            Redis.Set("test2", 1);

            var resp1 = Redis.RenameNx("test", "test2");
            Assert.IsFalse(resp1);

            var resp2 = Redis.RenameNx("test", "test3");
            Assert.IsTrue(resp2);

            Redis.Del("test", "test2", "test3");
        }

        // RESTORE

        [TestMethod, TestCategory("Keys")]
        public void TestSort()
        {
            Redis.Del("test", "test2", "test:weight_0", "test:weight_1", "test:weight_6", "test:weight_9", "test:weight_11", "test:obj_0", "test:obj_1", "test:obj_6", "test:obj_9", "test:obj_11");

            var list = new object[] { 6, 1, 9, 0, 11 };
            Redis.RPush("test", list);
            
            // simple
            var resp1 = Redis.Sort("test");
            Assert.AreEqual("0", resp1[0]);
            Assert.AreEqual("1", resp1[1]);
            Assert.AreEqual("6", resp1[2]);
            Assert.AreEqual("9", resp1[3]);
            Assert.AreEqual("11", resp1[4]);

            // desc
            var resp2 = Redis.Sort("test", dir: RedisSortDir.Desc);
            Assert.AreEqual("11", resp2[0]);
            Assert.AreEqual("9", resp2[1]);
            Assert.AreEqual("6", resp2[2]);
            Assert.AreEqual("1", resp2[3]);
            Assert.AreEqual("0", resp2[4]);

            // by external key
            Redis.Set("test:weight_6", 0);
            Redis.Set("test:weight_1", 5);
            Redis.Set("test:weight_9", 1);
            Redis.Set("test:weight_0", 11);
            Redis.Set("test:weight_11", 9);
            var resp3 = Redis.Sort("test", by: "test:weight_*");
            Assert.AreEqual("6", resp3[0]);
            Assert.AreEqual("9", resp3[1]);
            Assert.AreEqual("1", resp3[2]);
            Assert.AreEqual("11", resp3[3]);
            Assert.AreEqual("0", resp3[4]);

            // by external key, with external key
            Redis.Set("test:obj_6", "abc");
            Redis.Set("test:obj_1", "def");
            Redis.Set("test:obj_9", "ghi");
            Redis.Set("test:obj_0", "jkl");
            Redis.Set("test:obj_11", "mno");
            var resp4 = Redis.Sort("test", by: "test:weight_*", get: new[] { "test:obj_*" });
            Assert.AreEqual("abc", resp4[0]);
            Assert.AreEqual("ghi", resp4[1]);
            Assert.AreEqual("def", resp4[2]);
            Assert.AreEqual("mno", resp4[3]);
            Assert.AreEqual("jkl", resp4[4]);

            // legographically
            var resp5 = Redis.Sort("test", isAlpha: true);
            Assert.AreEqual("0", resp5[0]);
            Assert.AreEqual("1", resp5[1]);
            Assert.AreEqual("11", resp5[2]);
            Assert.AreEqual("6", resp5[3]);
            Assert.AreEqual("9", resp5[4]);

            // limit
            var resp6 = Redis.Sort("test", offset: 1, count: 2);
            Assert.AreEqual("1", resp6[0]);
            Assert.AreEqual("6", resp6[1]);

            // store
            var resp7 = Redis.SortAndStore("test", "test2");
            Assert.AreEqual(list.Length, resp7);

            Redis.Del("test", "test2", "test:weight_0", "test:weight_1", "test:weight_6", "test:weight_9", "test:weight_11", "test:obj_0", "test:obj_1", "test:obj_6", "test:obj_9", "test:obj_11");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestType()
        {
            Redis.Del("test");

            Redis.Set("test", 1);
            var resp1 = Redis.Type("test");
            Assert.AreEqual("string", resp1);

            Redis.Del("test");
        }
    }
}
