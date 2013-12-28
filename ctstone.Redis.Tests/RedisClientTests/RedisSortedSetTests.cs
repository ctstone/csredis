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
            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Assert.AreEqual(2, Redis.ZAdd("test1", "1", "one", "2", "two"));
                Assert.AreEqual(2, Redis.ZAdd("test2", new[] 
                { 
                    Tuple.Create(1.5, "one"),
                    Tuple.Create(2.5, "two"),
                }));
                Assert.AreEqual(2, Redis.ZAdd("test3", new[] 
                { 
                    Tuple.Create(10, "one"),
                    Tuple.Create(20, "two"),
                }));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZCard()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two");
                Assert.AreEqual(2, Redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZCount()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, Redis.ZCount("test1", Double.MinValue, Double.MaxValue));
                Assert.AreEqual(4, Redis.ZCount("test1", 1, 4));
                Assert.AreEqual(3, Redis.ZCount("test1", 1, 4, exclusiveMin:true));
                Assert.AreEqual(3, Redis.ZCount("test1", 1, 4, exclusiveMax:true));
                Assert.AreEqual(2, Redis.ZCount("test1", 1, 4, exclusiveMin:true, exclusiveMax: true));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZIncrBy()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, Redis.ZIncrBy("test1", 2, "two"));
                Assert.AreEqual(0, Redis.ZIncrBy("test1", -1, "one"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZInterStore()
        {
            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two");
                Redis.ZAdd("test2", "10", "one", "20", "two", "30", "three");
                Assert.AreEqual(2, Redis.ZInterStore("test3", null, RedisAggregate.Sum, "test1", "test2"));
                Assert.IsTrue(Redis.Exists("test3"));
                var resp = Redis.ZRange("test3", 0, -1, true);
            }

            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one");
                Redis.ZAdd("test2", "10", "one", "20", "two");
                Assert.AreEqual(1, Redis.ZInterStore("test3", new[] { 2.0, 100.0 }, RedisAggregate.Sum, "test1", "test2"));
                var resp = Redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual((1 * 2.0 + 10 * 100).ToString(), resp[1]);
            }

            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one");
                Redis.ZAdd("test2", "10", "one", "20", "two");
                Assert.AreEqual(1, Redis.ZInterStore("test3", new[] { 2.0, 100.0 }, RedisAggregate.Max, "test1", "test2"));
                var resp = Redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("1000", resp[1]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRange()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, Redis.ZRange("test1", 1, 2).Length);
                Assert.AreEqual(2 * 2, Redis.ZRange("test1", 1, 2, withScores:true).Length);
                Assert.AreEqual("one", Redis.ZRange("test1", 0, -1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRangeByScore()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, Redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue).Length);
                Assert.AreEqual(2, Redis.ZRangeByScore("test1", 1, 4, exclusiveMin:true, exclusiveMax:true).Length);
                Assert.AreEqual(2, Redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue, offset:1, count:2).Length);
                Assert.AreEqual(4*2, Redis.ZRangeByScore("test1", Double.MinValue, Double.MaxValue, withScores:true).Length);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRank()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, Redis.ZRank("test1", "three"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRem()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, Redis.ZRem("test1", "one", "two", "junk"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRemRangeByRank()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(3, Redis.ZRemRangeByRank("test1", 0, 2));
                Assert.AreEqual(1, Redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRemRangeByScore()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, Redis.ZRemRangeByScore("test1", Double.MinValue, 2));
                Assert.AreEqual(2, Redis.ZCard("test1"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRange()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(2, Redis.ZRevRange("test1", 1, 2).Length);
                Assert.AreEqual(2 * 2, Redis.ZRevRange("test1", 1, 2, withScores: true).Length);
                Assert.AreEqual("four", Redis.ZRevRange("test1", 0, -1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRangeByScore()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, Redis.ZRevRangeByScore("test1", Double.MaxValue, Double.MinValue).Length);
                Assert.AreEqual(4*2, Redis.ZRevRangeByScore("test1", Double.MaxValue, Double.MinValue, withScores:true).Length);
                Assert.AreEqual(2, Redis.ZRevRangeByScore("test1", 4, 3).Length);
                Assert.AreEqual(1, Redis.ZRevRangeByScore("test1", 4, 2, exclusiveMax:true, exclusiveMin:true).Length);
                Assert.AreEqual(2, Redis.ZRevRangeByScore("test1", 4, 1, offset:1, count:2).Length);
                Assert.AreEqual("four", Redis.ZRevRangeByScore("test1", 4, 1)[0]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRevRank()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(0, Redis.ZRevRank("test1", "four"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZScore()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three", "4", "four");
                Assert.AreEqual(4, Redis.ZScore("test1", "four"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZUnionStore()
        {
            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                Redis.ZAdd("test1", "30", "three", "40", "four");
                Assert.AreEqual(4, Redis.ZUnionStore("test3", null, null, "test1", "test2"));
                Assert.IsTrue(Redis.Exists("test3"));
            }

            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                Redis.ZAdd("test1", "30", "three", "40", "four");
                Assert.AreEqual(4, Redis.ZUnionStore("test3", new[] { 2.0, 10.0 }, null, "test1", "test2"));
                var resp = Redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("one", resp[0]);
                Assert.AreEqual("2", resp[1]);
            }

            using (new RedisTestKeys(Redis, "test1", "test2", "test3"))
            {
                Redis.ZAdd("test1", "1", "one", "2", "two", "3", "three");
                Redis.ZAdd("test2", "30", "three", "40", "four");
                Assert.AreEqual(4, Redis.ZUnionStore("test3", new[] { 2.0, 10.0 }, RedisAggregate.Min, "test1", "test2"));
                var resp = Redis.ZRange("test3", 0, -1, true);
                Assert.AreEqual("one", resp[0]);
                Assert.AreEqual("2", resp[1]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZScan()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.ZAdd("test1", "0", "abc", "1", "def");
                var scan = Redis.ZScan("test1", 0);
                Assert.AreEqual(2, scan.Items.Count);
                Assert.AreEqual(0, scan.Cursor);
            }
        }
    }
}
