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
            using (new RedisTestKeys(_redis, "test"))
            {
                var my_obj = RedisGenericTest.Create();
                Assert.AreEqual("OK", _redis.HMSet("test", my_obj));

                var redis_obj = _redis.HGetAll<RedisGenericTest>("test");
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
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHashCreate_Dict()
        {
            using (new RedisTestKeys(_redis, "test"))
            {
                Assert.AreEqual("OK", _redis.HMSet("test", new Dictionary<string, string>
                {
                    { "key1", "value1" },
                    { "key2", "value2" },
                    { "key3", null },
                }));

                var redis_hash = _redis.HGetAll("test");
                Assert.AreEqual("value1", redis_hash["key1"]);
                Assert.AreEqual("value2", redis_hash["key2"]);
                Assert.IsFalse(redis_hash.ContainsKey("key3"));
            }
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHashCreate_KeyValues()
        {
            using (new RedisTestKeys(_redis, "test"))
            {
                Assert.AreEqual("OK", _redis.HMSet("test", new[] 
                {
                    "key1", 
                    "value1", 
                    "key2", 
                    "value2",
                    "key3",
                    null,
                }));

                var redis_hash = _redis.HGetAll("test");
                Assert.AreEqual("value1", redis_hash["key1"]);
                Assert.AreEqual("value2", redis_hash["key2"]);
                Assert.IsFalse(redis_hash.ContainsKey("key3"));
            }
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHDel()
        {
            _redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = _redis.HMSet("test", my_obj);

            var hdel_result = _redis.HDel("test", "StringField", "CharField");
            Assert.AreEqual(2, hdel_result);

            var hgetall_result = _redis.HGetAll("test");
            Assert.IsFalse(hgetall_result.ContainsKey("StringField"));
            Assert.IsFalse(hgetall_result.ContainsKey("CharField"));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHExists()
        {
            _redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = _redis.HMSet("test", my_obj);

            var hexists_result1 = _redis.HExists("test", "StringField");
            Assert.IsTrue(hexists_result1);

            var hexists_result2 = _redis.HExists("test", "JunkField");
            Assert.IsFalse(hexists_result2);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHGet()
        {
            _redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = _redis.HMSet("test", my_obj);

            var hget_string_result = _redis.HGet("test", "StringField");
            Assert.AreEqual(my_obj.StringField, hget_string_result);

            var hget_int_result = _redis.HGet("test", "IntField");
            Assert.AreEqual(my_obj.IntField.ToString(), hget_int_result);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHIncrBy()
        {
            _redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = _redis.HMSet("test", my_obj);

            var hincrby_result = _redis.HIncrBy("test", "IntField", 3);
            Assert.AreEqual(my_obj.IntField + 3, hincrby_result);

            var hincrby_result2 = _redis.HIncrBy("test", "IntField", -3);
            Assert.AreEqual(my_obj.IntField + 3 - 3, hincrby_result2);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHIncrByFloat()
        {
            _redis.Del("test");

            RedisGenericTest my_obj = RedisGenericTest.Create();
            var hmsset_result = _redis.HMSet("test", my_obj);

            var hincrby_result = _redis.HIncrByFloat("test", "FloatField", 3.4);
            Assert.AreEqual(my_obj.FloatField + 3.4F, (float)hincrby_result);

            var hincrby_result2 = _redis.HIncrByFloat("test", "FloatField", -3.4);
            Assert.AreEqual(my_obj.FloatField + 3.4F - 3.4F, (float)hincrby_result2);

            var hincrby_result3 = _redis.HIncrByFloat("test", "DoubleField", 1.2);
            Assert.AreEqual(my_obj.DoubleField + 1.2, hincrby_result3);
                
            var hincrby_result4 = _redis.HIncrByFloat("test", "DoubleField", -3.4);
            Assert.AreEqual(my_obj.DoubleField + 1.2 - 3.4, hincrby_result4);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHKeys()
        {
            _redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            _redis.HMSet("test", dict);

            var hkeys_result = _redis.HKeys("test");
            Assert.AreEqual(dict.Count, hkeys_result.Length);
            foreach (var k in dict.Keys)
                Assert.IsTrue(hkeys_result.Contains(k));

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHLen()
        {
            _redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            _redis.HMSet("test", dict);

            var hlen_result = _redis.HLen("test");
            Assert.AreEqual(dict.Count, hlen_result);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHMGet_Array()
        {
            _redis.Del("test");

            var obj = RedisGenericTest.Create();
            _redis.HMSet("test", obj);

            var res = _redis.HMGet("test", "StringField", "DoubleField");
            Assert.AreEqual(obj.StringField, res[0]);
            Assert.AreEqual(obj.DoubleField.ToString(), res[1]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHSet()
        {
            _redis.Del("test");

            var obj = RedisGenericTest.Create();
            _redis.HMSet("test", obj);

            var res_update = _redis.HSet("test", "StringField", "update");
            Assert.IsFalse(res_update);

            var res_new = _redis.HSet("test", "NewField", "new");
            Assert.IsTrue(res_new);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHSetNx()
        {
            _redis.Del("test");

            var obj = RedisGenericTest.Create();
            _redis.HMSet("test", obj);

            var res_update = _redis.HSetNx("test", "StringField", "update");
            Assert.IsFalse(res_update);
            var res_dict1 = _redis.HGetAll("test");
            Assert.AreEqual(obj.StringField, res_dict1["StringField"]);

            var res_new = _redis.HSetNx("test", "NewField", "new");
            Assert.IsTrue(res_new);
            var res_dict2 = _redis.HGetAll("test");
            Assert.AreEqual("new", res_dict2["NewField"]);

            _redis.Del("test");
        }

        [TestMethod, TestCategory("Hash")]
        public void TestHVals()
        {
            _redis.Del("test");

            var dict = new Dictionary<string, string>
            {
                {"Key1", "Value1" },
                {"Key2", "Value2" },
            };
            _redis.HMSet("test", dict);

            var vals = _redis.HVals("test");
            foreach (var v in dict.Values)
                Assert.IsTrue(vals.Contains(v));

            _redis.Del("test");

            // non-exist
            var non_hash_vals = _redis.HVals(String.Format("test_{0}", Guid.NewGuid()));
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
