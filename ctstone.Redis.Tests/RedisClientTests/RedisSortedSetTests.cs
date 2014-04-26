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

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRangeByLex()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Assert.AreEqual(7, Redis.ZAdd("test1",
                    Tuple.Create(0, "a"),
                    Tuple.Create(0, "b"),
                    Tuple.Create(0, "c"),
                    Tuple.Create(0, "d"),
                    Tuple.Create(0, "e"),
                    Tuple.Create(0, "f"),
                    Tuple.Create(0, "g")));

                var r1 = Redis.ZRangeByLex("test1", "-", "[c");
                Assert.AreEqual(3, r1.Length);
                Assert.AreEqual("a", r1[0]);
                Assert.AreEqual("b", r1[1]);
                Assert.AreEqual("c", r1[2]);

                var r2 = Redis.ZRangeByLex("test1", "[aaa", "(g");
                Assert.AreEqual(5, r2.Length);
                Assert.AreEqual("b", r2[0]);
                Assert.AreEqual("c", r2[1]);
                Assert.AreEqual("d", r2[2]);
                Assert.AreEqual("e", r2[3]);
                Assert.AreEqual("f", r2[4]);
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZLexCount()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Assert.AreEqual(5, Redis.ZAdd("test1",
                    Tuple.Create(0, "a"),
                    Tuple.Create(0, "b"),
                    Tuple.Create(0, "c"),
                    Tuple.Create(0, "d"),
                    Tuple.Create(0, "e")));

                Assert.AreEqual(2, Redis.ZAdd("test1",
                    Tuple.Create(0, "f"),
                    Tuple.Create(0, "g")));

                Assert.AreEqual(7, Redis.ZLexCount("test1", "-", "+"));
                Assert.AreEqual(5, Redis.ZLexCount("test1", "[b", "[f"));
            }
        }

        [TestMethod, TestCategory("Sorted Sets")]
        public void TestZRemRangeByLex()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Assert.AreEqual(5, Redis.ZAdd("test1",
                    Tuple.Create(0, "aaaa"),
                    Tuple.Create(0, "b"),
                    Tuple.Create(0, "c"),
                    Tuple.Create(0, "d"),
                    Tuple.Create(0, "e")));

                Assert.AreEqual(5, Redis.ZAdd("test1",
                    Tuple.Create(0, "foo"),
                    Tuple.Create(0, "zap"),
                    Tuple.Create(0, "zap"),
                    Tuple.Create(0, "zip"),
                    Tuple.Create(0, "ALPHA"),
                    Tuple.Create(0, "alpha")));

                var r1 = Redis.ZRange("test1", 0, -1);
                Assert.AreEqual(10, r1.Length);
                Assert.AreEqual("ALPHA", r1[0]);
                Assert.AreEqual("aaaa", r1[1]);
                Assert.AreEqual("alpha", r1[2]);
                Assert.AreEqual("b", r1[3]);
                Assert.AreEqual("c", r1[4]);
                Assert.AreEqual("d", r1[5]);
                Assert.AreEqual("e", r1[6]);
                Assert.AreEqual("foo", r1[7]);
                Assert.AreEqual("zap", r1[8]);
                Assert.AreEqual("zip", r1[9]);

                Assert.AreEqual(6, Redis.ZRemRangeByLex("test1", "[alpha", "[omega"));

                var r2 = Redis.ZRange("test1", 0, -1);
                Assert.AreEqual(4, r2.Length);
                Assert.AreEqual("ALPHA", r2[0]);
                Assert.AreEqual("aaaa", r2[1]);
                Assert.AreEqual("zap", r2[2]);
                Assert.AreEqual("zip", r2[3]);
            }
        }
    }
}
