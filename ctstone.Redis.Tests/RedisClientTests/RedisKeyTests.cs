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
            _redis.Del("test", "test1", "test2", "test3", "test4", "test5");
            _redis.MSet(new[] 
            {
                Tuple.Create("test", "t"),
                Tuple.Create("test1", "t1"),
                Tuple.Create("test2", "t2"),
                Tuple.Create("test3", "t3"),
                Tuple.Create("test4", "t4"),
            });

            var del1 = _redis.Del("test", "test1", "test2");
            Assert.AreEqual(3, del1);

            var del2 = _redis.Del("test3", "test4", "test5");
            Assert.AreEqual(2, del2);
        }

        [TestMethod, TestCategory("Keys")]
        public void TestDump()
        {
            _redis.Del("test");
            _redis.Set("test", 10);
            var res = _redis.Dump("test");
            byte[] expected = new byte[] 
            {
                0x0, 0xC0, 0xA, 0x06, 0x0, 0xF8, 0x72, 0x3F, 0xC5, 0xFB, 0xFB, 0x5F, 0x28
            };

            for (int i = 0; i < res.Length; i++)
                Assert.AreEqual(expected[i], res[i]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExists()
        {
            _redis.Del("test");

            var res1 = _redis.Exists("test");
            Assert.IsFalse(res1);

            _redis.Set("test", 1);
            
            var res2 = _redis.Exists("test");
            Assert.IsTrue(res2);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpire()
        {
            _redis.Del("test");

            var res1 = _redis.Expire("test", 10);
            Assert.IsFalse(res1);

            _redis.Set("test", "t1");
            var res2 = _redis.Expire("test", 10);
            Assert.IsTrue(res2);

            var res3 = _redis.Ttl("test");
            Assert.IsTrue(res3 > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestExpireAt()
        {
            _redis.Del("test");

            var server_time = _redis.Time();

            var res1 = _redis.ExpireAt("test", server_time + TimeSpan.FromSeconds(10));
            Assert.IsFalse(res1);

            _redis.Set("test", 1);

            var res2 = _redis.ExpireAt("test", server_time + TimeSpan.FromSeconds(10));
            Assert.IsTrue(res2);

            var res3 = _redis.Ttl("test");
            Assert.IsTrue(res3 > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestKeys()
        {
            _redis.Del("test:test", "test:tast", "test:tost", "test:tist", "test:tust");

            _redis.MSet("test:test", "1", "test:tast", "1", "test:tost", "1", "test:tist", "1", "test:tust", "1");

            var res1 = _redis.Keys("test:*");
            Assert.AreEqual(5, res1.Length);

            var res2 = _redis.Keys("test:t?st");
            Assert.AreEqual(5, res2.Length);

            var res3 = _redis.Keys("test:t[ae]st");
            Assert.AreEqual(2, res3.Length);

            _redis.Del("test:test", "test:tast", "test:tost", "test:tist", "test:tust");
        }

        // MIGRATE

        // MOVE

        // OBJECT

        [TestMethod, TestCategory("Keys")]
        public void TestPersist()
        {
            _redis.Del("test");
            
            _redis.Set("test", "t1");
            _redis.Expire("test", 10);

            var res1 = _redis.Ttl("test");
            Assert.IsTrue(res1 > 0);

            var res2 = _redis.Persist("test");
            Assert.IsTrue(res2);

            var res3 = _redis.Ttl("test");
            Assert.AreEqual(-1, res3);

            _redis.Del("test");

            var res4 = _redis.Persist("test");
            Assert.IsFalse(res4);
        }

        [TestMethod, TestCategory("Keys")]
        public void TestPExpire()
        {
            _redis.Del("test");

            var res1 = _redis.PExpire("test", 10000);
            Assert.IsFalse(res1);

            _redis.Set("test", "t1");
            var res2 = _redis.PExpire("test", 10000);
            Assert.IsTrue(res2);

            var res3 = _redis.PTtl("test");
            Assert.IsTrue(res3 > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestPExpireAt()
        {
            _redis.Del("test");

            var server_time = _redis.Time();

            var res1 = _redis.PExpireAt("test", server_time + TimeSpan.FromMilliseconds(10000));
            Assert.IsFalse(res1);

            _redis.Set("test", 1);

            var res2 = _redis.PExpireAt("test", server_time + TimeSpan.FromMilliseconds(10000));
            Assert.IsTrue(res2);

            var res3 = _redis.PTtl("test");
            Assert.IsTrue(res3 > 0);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRandomKey()
        {
            _redis.Del("test");

            _redis.Set("test", 1);
            var res = _redis.RandomKey();
            Assert.IsNotNull(res);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRename()
        {
            _redis.Del("test", "test2");

            string guid = Guid.NewGuid().ToString();
            _redis.Set("test", guid);
            var resp1 = _redis.Rename("test", "test2");
            Assert.AreEqual("OK", resp1);

            var resp2 = _redis.Exists("test");
            Assert.IsFalse(resp2);

            var resp3 = _redis.Get("test2");
            Assert.AreEqual(guid, resp3);

            _redis.Del("test", "test2");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestRenameX()
        {
            _redis.Del("test", "test2", "test3");

            string guid = Guid.NewGuid().ToString();
            _redis.Set("test", guid);
            _redis.Set("test2", 1);

            var resp1 = _redis.RenameNx("test", "test2");
            Assert.IsFalse(resp1);

            var resp2 = _redis.RenameNx("test", "test3");
            Assert.IsTrue(resp2);

            _redis.Del("test", "test2", "test3");
        }

        // RESTORE

        [TestMethod, TestCategory("Keys")]
        public void TestSort()
        {
            _redis.Del("test", "test2", "test:weight_0", "test:weight_1", "test:weight_6", "test:weight_9", "test:weight_11", "test:obj_0", "test:obj_1", "test:obj_6", "test:obj_9", "test:obj_11");

            var list = new object[] { 6, 1, 9, 0, 11 };
            _redis.RPush("test", list);
            
            // simple
            var resp1 = _redis.Sort("test");
            Assert.AreEqual("0", resp1[0]);
            Assert.AreEqual("1", resp1[1]);
            Assert.AreEqual("6", resp1[2]);
            Assert.AreEqual("9", resp1[3]);
            Assert.AreEqual("11", resp1[4]);

            // desc
            var resp2 = _redis.Sort("test", dir: RedisSortDir.Desc);
            Assert.AreEqual("11", resp2[0]);
            Assert.AreEqual("9", resp2[1]);
            Assert.AreEqual("6", resp2[2]);
            Assert.AreEqual("1", resp2[3]);
            Assert.AreEqual("0", resp2[4]);

            // by external key
            _redis.Set("test:weight_6", 0);
            _redis.Set("test:weight_1", 5);
            _redis.Set("test:weight_9", 1);
            _redis.Set("test:weight_0", 11);
            _redis.Set("test:weight_11", 9);
            var resp3 = _redis.Sort("test", by: "test:weight_*");
            Assert.AreEqual("6", resp3[0]);
            Assert.AreEqual("9", resp3[1]);
            Assert.AreEqual("1", resp3[2]);
            Assert.AreEqual("11", resp3[3]);
            Assert.AreEqual("0", resp3[4]);

            // by external key, with external key
            _redis.Set("test:obj_6", "abc");
            _redis.Set("test:obj_1", "def");
            _redis.Set("test:obj_9", "ghi");
            _redis.Set("test:obj_0", "jkl");
            _redis.Set("test:obj_11", "mno");
            var resp4 = _redis.Sort("test", by: "test:weight_*", get: new[] { "test:obj_*" });
            Assert.AreEqual("abc", resp4[0]);
            Assert.AreEqual("ghi", resp4[1]);
            Assert.AreEqual("def", resp4[2]);
            Assert.AreEqual("mno", resp4[3]);
            Assert.AreEqual("jkl", resp4[4]);

            // legographically
            var resp5 = _redis.Sort("test", isAlpha: true);
            Assert.AreEqual("0", resp5[0]);
            Assert.AreEqual("1", resp5[1]);
            Assert.AreEqual("11", resp5[2]);
            Assert.AreEqual("6", resp5[3]);
            Assert.AreEqual("9", resp5[4]);

            // limit
            var resp6 = _redis.Sort("test", offset: 1, count: 2);
            Assert.AreEqual("1", resp6[0]);
            Assert.AreEqual("6", resp6[1]);

            // store
            var resp7 = _redis.SortAndStore("test", "test2");
            Assert.AreEqual(list.Length, resp7);

            _redis.Del("test", "test2", "test:weight_0", "test:weight_1", "test:weight_6", "test:weight_9", "test:weight_11", "test:obj_0", "test:obj_1", "test:obj_6", "test:obj_9", "test:obj_11");
        }

        [TestMethod, TestCategory("Keys")]
        public void TestType()
        {
            _redis.Del("test");

            _redis.Set("test", 1);
            var resp1 = _redis.Type("test");
            Assert.AreEqual("string", resp1);

            _redis.Del("test");
        }
    }
}
