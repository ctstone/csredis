using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using ctstone.Redis;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class RedisSortedSetTests : RedisTestBase
    {
        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZAdd()
        {
            using (new RedisTestKeys(_redis, "test1", "test2"))
            {
                Assert.AreEqual(2, _redis.ZAdd("test1", "1", "one", "2", "two"));
                Assert.AreEqual(2, _redis.ZAdd("test2", new[] 
                { 
                    Tuple.Create(1.5, "one"),
                    Tuple.Create(2.5, "two"),
                }));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZCard()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two");
                Assert.AreEqual(2, _redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZCount()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, _redis.ZCount("test1", Double.MinValue, Double.MaxValue));
                Assert.AreEqual(4, _redis.ZCount("test1", 1, 4));
                Assert.AreEqual(3, _redis.ZCount("test1", 1, 4, exclusiveMin:true));
                Assert.AreEqual(3, _redis.ZCount("test1", 1, 4, exclusiveMax:true));
                Assert.AreEqual(2, _redis.ZCount("test1", 1, 4, exclusiveMin:true, exclusiveMax: true));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZIncrBy()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, _redis.ZIncrBy("test1", 2, "two"));
                Assert.AreEqual(0, _redis.ZIncrBy("test1", -1, "one"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZInterStore()
        {
            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two");
                _redis.ZAdd("test2", "10", "one", "20", "two", "30", "three");
                Assert.AreEqual(2, _redis.ZInterStore("test3", null, RedisAggregate.Sum, "test1", "test2"));
                Assert.IsTrue(_redis.Exists("test3"));
                var resp = _redis.ZRange("test3", 0, -1, true);
            }

            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one");
                _redis.ZAdd("test2", "10", "one", "20", "two");
                Assert.AreEqual(1, _redis.ZInterStore("test3", new[] { 2.0, 100.0 }, RedisAggregate.Sum, "test1", "test2"));
                var resp = _redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual((1 * 2.0 + 10 * 100).ToString(), resp[1]);
            }

            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one");
                _redis.ZAdd("test2", "10", "one", "20", "two");
                Assert.AreEqual(1, _redis.ZInterStore("test3", new[] { 2.0, 100.0 }, RedisAggregate.Max, "test1", "test2"));
                var resp = _redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("1000", resp[1]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRange()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, _redis.ZRange("test1", 1, 2).Length);
                Assert.AreEqual(2 * 2, _redis.ZRange("test1", 1, 2, withScores:true).Length);
                Assert.AreEqual("one", _redis.ZRange("test1", 0, -1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRangeByScore()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, _redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue).Length);
                Assert.AreEqual(2, _redis.ZRangeByScore("test1", 1, 4, exclusiveMin:true, exclusiveMax:true).Length);
                Assert.AreEqual(2, _redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue, offset:1, count:2).Length);
                Assert.AreEqual(4*2, _redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue, withScores:true).Length);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRank()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, _redis.ZRank("test1", "three"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRem()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, _redis.ZRem("test1", "one", "two", "junk"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRemRangeByRank()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(3, _redis.ZRemRangeByRank("test1", 0, 2));
                Assert.AreEqual(1, _redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRemRangeByScore()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, _redis.ZRemRangeByScore("test1", Double.MinValue, 2));
                Assert.AreEqual(2, _redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRange()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, _redis.ZRevRange("test1", 1, 2).Length);
                Assert.AreEqual(2 * 2, _redis.ZRevRange("test1", 1, 2, withScores: true).Length);
                Assert.AreEqual("four", _redis.ZRevRange("test1", 0, -1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRangeByScore()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, _redis.ZRevRangeByScore("test1", Double.MaxValue, Double.MinValue).Length);
                Assert.AreEqual(4*2, _redis.ZRevRangeByScore("test1", Double.MaxValue, Double.MinValue, withScores:true).Length);
                Assert.AreEqual(2, _redis.ZRevRangeByScore("test1", 4, 3).Length);
                Assert.AreEqual(1, _redis.ZRevRangeByScore("test1", 4, 2, exclusiveMax:true, exclusiveMin:true).Length);
                Assert.AreEqual(2, _redis.ZRevRangeByScore("test1", 4, 1, offset:1, count:2).Length);
                Assert.AreEqual("four", _redis.ZRevRangeByScore("test1", 4, 1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRank()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(0, _redis.ZRevRank("test1", "four"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZScore()
        {
            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, _redis.ZScore("test1", "four"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZUnionStore()
        {
            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                _redis.ZAdd("test1", "30", "three", "40", "four");
                Assert.AreEqual(4, _redis.ZUnionStore("test3", null, null, "test1", "test2"));
                Assert.IsTrue(_redis.Exists("test3"));
            }

            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                _redis.ZAdd("test1", "30", "three", "40", "four");
                Assert.AreEqual(4, _redis.ZUnionStore("test3", new[] { 2.0, 10.0 }, null, "test1", "test2"));
                var resp = _redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("one", resp[0]);
                Assert.AreEqual("2", resp[1]);
            }

            using (new RedisTestKeys(_redis, "test1", "test2", "test3"))
            {
                _redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                _redis.ZAdd("test2", "30", "three", "40", "four");
                Assert.AreEqual(4, _redis.ZUnionStore("test3", new[] { 2.0, 10.0 }, RedisAggregate.Min, "test1", "test2"));
                var resp = _redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("one", resp[0]);
                Assert.AreEqual("2", resp[1]);
            }
        }
    }
}
