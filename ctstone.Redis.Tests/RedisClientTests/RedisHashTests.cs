using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisHashTests : RedisTestBase
    {
        [TestMethod, TestCategory("Hash")]
        public void TestHashCreate_Generic()
        {
            using (new RedisTestKeys(Redis, "test"))
            {
                var my_obj = RedisGenericTest.Create();
                Assert.AreEqual("OK", Redis.HMSet("test", my_obj));

                var redis_obj = Redis.HGetAll<RedisGenericTest>("test");
                Assert.AreEqual(my_obj.StringField, redis_obj.StringField);
                Assert.AreEqual(my_obj.IntField, redis_obj.IntField);
                Assert.AreEqual(my_obj.UIntField, redis_obj.UIntField);
                Assert.AreEqual(my_obj.DoubleField, redis_obj.DoubleField);
                Assert.AreEqual(my_obj.FloatField, redis_obj.FloatField);
                Assert.AreEqual(my_obj.BoolField, redis_obj.BoolField);
                Assert.AreEqual(my_obj.LongField, redis_obj.LongField);
                Assert.AreEqual(my_obj.ULongField, redis_obj.ULongField);
                Assert.AreEqual(my_obj.ShortField, redis_obj.ShortField);
                Assert.AreEqual(my_obj.UShortField, redis_obj.UShortField);
                Assert.AreEqual(my_obj.ByteField, redis_obj.ByteField);
                Assert.AreEqual(my_obj.SByteField, redis_obj.SByteField);
                Assert.AreEqual(my_obj.CharField, redis_obj.CharField);
                Assert.IsNull(redis_obj.NullField);
                Assert.AreEqual(my_obj.DateTimeField.ToString(), redis_obj.DateTimeField.ToString());
                Assert.AreEqual(my_obj.DateTimeOffsetField.ToString(), redis_obj.DateTimeOffsetField.ToString());
            }

            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.IsNull(Redis.HGetAll<RedisGenericTest>("test"));
            }
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHashCreate_Dict()
        {
            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.HMSet("test", new Dictionary<string, string>
                {
                    { "key1", "value1" },
                    { "key2", "value2" },
                    { "key3", null },
                }));

                var redis_hash = Redis.HGetAll("test");
                Assert.AreEqual("value1", redis_hash["key1"]);
                Assert.AreEqual("value2", redis_hash["key2"]);
                Assert.IsFalse(redis_hash.ContainsKey("key3"));
            }
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHashCreate_KeyValues()
        {
            using (new RedisTestKeys(Redis, "test"))
            {
                Assert.AreEqual("OK", Redis.HMSet("test", new[] 
                {
                    "key1", 
                    "value1", 
                    "key2", 
                    "value2",
                    "key3",
                    null,
                }));

                var redis_hash = Redis.HGetAll("test");
                Assert.AreEqual("value1", redis_hash["key1"]);
                Assert.AreEqual("value2", redis_hash["key2"]);
                Assert.IsFalse(redis_hash.ContainsKey("key3"));
            }
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHDel()
        {
            Redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = Redis.HMSet("test", my_obj);

            var hdel_result = Redis.HDel("test", "StringField", "CharField");
            Assert.AreEqual(2, hdel_result);

            var hgetall_result = Redis.HGetAll("test");
            Assert.IsFalse(hgetall_result.ContainsKey("StringField"));
            Assert.IsFalse(hgetall_result.ContainsKey("CharField"));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHExists()
        {
            Redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = Redis.HMSet("test", my_obj);

            var hexists_result1 = Redis.HExists("test", "StringField");
            Assert.IsTrue(hexists_result1);

            var hexists_result2 = Redis.HExists("test", "JunkField");
            Assert.IsFalse(hexists_result2);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHGet()
        {
            Redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = Redis.HMSet("test", my_obj);

            var hget_string_result = Redis.HGet("test", "StringField");
            Assert.AreEqual(my_obj.StringField, hget_string_result);

            var hget_int_result = Redis.HGet("test", "IntField");
            Assert.AreEqual(my_obj.IntField.ToString(), hget_int_result);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHIncrBy()
        {
            Redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = Redis.HMSet("test", my_obj);

            var hincrby_result = Redis.HIncrBy("test", "IntField", 3);
            Assert.AreEqual(my_obj.IntField + 3, hincrby_result);

            var hincrby_result2 = Redis.HIncrBy("test", "IntField", -3);
            Assert.AreEqual(my_obj.IntField + 3 - 3, hincrby_result2);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHIncrByFloat()
        {
            Redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = Redis.HMSet("test", my_obj);

            var hincrby_result = Redis.HIncrByFloat("test", "FloatField", 3.4);
            Assert.AreEqual(my_obj.FloatField + 3.4F, (float)hincrby_result);

            var hincrby_result2 = Redis.HIncrByFloat("test", "FloatField", -3.4);
            Assert.AreEqual(my_obj.FloatField + 3.4F - 3.4F, (float)hincrby_result2);

            var hincrby_result3 = Redis.HIncrByFloat("test", "DoubleField", 1.2);
            Assert.AreEqual(my_obj.DoubleField + 1.2, hincrby_result3);
                
            var hincrby_result4 = Redis.HIncrByFloat("test", "DoubleField", -3.4);
            Assert.AreEqual(my_obj.DoubleField + 1.2 - 3.4, hincrby_result4);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHKeys()
        {
            Redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            Redis.HMSet("test", dict);

            var hkeys_result = Redis.HKeys("test");
            Assert.AreEqual(dict.Count, hkeys_result.Length);
            foreach (var k in dict.Keys)
                Assert.IsTrue(hkeys_result.Contains(k));

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHLen()
        {
            Redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            Redis.HMSet("test", dict);

            var hlen_result = Redis.HLen("test");
            Assert.AreEqual(dict.Count, hlen_result);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHMGet_Array()
        {
            Redis.Del("test");

            var obj = RedisGenericTest.Create();
            Redis.HMSet("test", obj);

            var res = Redis.HMGet("test", "StringField", "DoubleField");
            Assert.AreEqual(obj.StringField, res[0]);
            Assert.AreEqual(obj.DoubleField.ToString(), res[1]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHSet()
        {
            Redis.Del("test");

            var obj = RedisGenericTest.Create();
            Redis.HMSet("test", obj);

            var res_update = Redis.HSet("test", "StringField", "update");
            Assert.IsFalse(res_update);

            var res_new = Redis.HSet("test", "NewField", "new");
            Assert.IsTrue(res_new);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHSetNx()
        {
            Redis.Del("test");

            var obj = RedisGenericTest.Create();
            Redis.HMSet("test", obj);

            var res_update = Redis.HSetNx("test", "StringField", "update");
            Assert.IsFalse(res_update);
            var res_dict1 = Redis.HGetAll("test");
            Assert.AreEqual(obj.StringField, res_dict1["StringField"]);

            var res_new = Redis.HSetNx("test", "NewField", "new");
            Assert.IsTrue(res_new);
            var res_dict2 = Redis.HGetAll("test");
            Assert.AreEqual("new", res_dict2["NewField"]);

            Redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHVals()
        {
            Redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            Redis.HMSet("test", dict);

            var vals = Redis.HVals("test");
            foreach (var v in dict.Values)
                Assert.IsTrue(vals.Contains(v));

            Redis.Del("test");

            // non-exist
            var non_hash_vals = Redis.HVals(String.Format("test_{0}", Guid.NewGuid()));
            Assert.IsNotNull(non_hash_vals);
            Assert.AreEqual(0, non_hash_vals.Length);
        }

        public class RedisGenericTest
        {
            public string StringField { get; set; }
            public int IntField { get; set; }
            public uint UIntField { get; set; }
            public double DoubleField { get; set; }
            public float FloatField { get; set; }
            public bool BoolField { get; set; }
            public long LongField { get; set; }
            public ulong ULongField { get; set; }
            public short ShortField { get; set; }
            public ushort UShortField { get; set; }
            public byte ByteField { get; set; }
            public sbyte SByteField { get; set; }
            public char CharField { get; set; }
            public DateTime DateTimeField { get; set; }
            public DateTimeOffset DateTimeOffsetField { get; set; }
            public string NullField { get; set; }

            public static RedisGenericTest Create()
            {
                return new RedisGenericTest
                {
                    StringField = "str",
                    IntField = 1,
                    UIntField = 2,
                    DoubleField = 3.3,
                    FloatField = 4.4F,
                    BoolField = true,
                    LongField = 5L,
                    ULongField = 6UL,
                    ShortField = 7,
                    UShortField = 8,
                    ByteField = 9,
                    SByteField = 10,
                    CharField = 'a',
                    DateTimeField = DateTime.UtcNow,
                    DateTimeOffsetField = DateTimeOffset.UtcNow,
                    NullField = null,
                };
            }
        }
    }
}
