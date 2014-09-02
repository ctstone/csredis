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
    public class SortedSetTests
    {
        [TestMethod, TestCategory("SortedSets")]
        public void TestZAdd_Array()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZAdd("test", "1.1", "test1", "2.2", "test2"));
                Assert.AreEqual("*6\r\n$4\r\nZADD\r\n$4\r\ntest\r\n$3\r\n1.1\r\n$5\r\ntest1\r\n$3\r\n2.2\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZAdd_Tuple()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZAdd("test", Tuple.Create(1.1, "test1"), Tuple.Create(2.2, "test2")));
                Assert.AreEqual("*6\r\n$4\r\nZADD\r\n$4\r\ntest\r\n$3\r\n1.1\r\n$5\r\ntest1\r\n$3\r\n2.2\r\n$5\r\ntest2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZCard()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZCard("test"));
                Assert.AreEqual("*2\r\n$5\r\nZCARD\r\n$4\r\ntest\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZCount()
        {
            string reply = ":2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZCount("test", 1, 3));
                Assert.AreEqual("*4\r\n$6\r\nZCOUNT\r\n$4\r\ntest\r\n$1\r\n1\r\n$1\r\n3\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZCount("test", Double.NegativeInfinity, Double.PositiveInfinity));
                Assert.AreEqual("*4\r\n$6\r\nZCOUNT\r\n$4\r\ntest\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZCount("test", 1, 3, exclusiveMin: true, exclusiveMax: true));
                Assert.AreEqual("*4\r\n$6\r\nZCOUNT\r\n$4\r\ntest\r\n$2\r\n(1\r\n$2\r\n(3\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZCount("test", "-inf", "+inf"));
                Assert.AreEqual("*4\r\n$6\r\nZCOUNT\r\n$4\r\ntest\r\n$4\r\n-inf\r\n$4\r\n+inf\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZCount("test", "(1", "(3"));
                Assert.AreEqual("*4\r\n$6\r\nZCOUNT\r\n$4\r\ntest\r\n$2\r\n(1\r\n$2\r\n(3\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZIncrby()
        {
            using (var mock = new FakeRedisSocket("$4\r\n3.14\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3.14, redis.ZIncrBy("test", 1.5, "test1"));
                Assert.AreEqual("*4\r\n$7\r\nZINCRBY\r\n$4\r\ntest\r\n$3\r\n1.5\r\n$5\r\ntest1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZInterStore()
        {
            string reply = ":2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZInterStore("destination", "key1", "key2" ));
                Assert.AreEqual("*5\r\n$11\r\nZINTERSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZInterStore("destination", weights: new[] { 1D, 2D }, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*8\r\n$11\r\nZINTERSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZInterStore("destination", weights: new[] { 1D, 2D }, aggregate:RedisAggregate.Max, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZINTERSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nMAX\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZInterStore("destination", weights: new[] { 1D, 2D }, aggregate: RedisAggregate.Min, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZINTERSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nMIN\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZInterStore("destination", weights: new[] { 1D, 2D }, aggregate: RedisAggregate.Sum, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZINTERSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nSUM\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZLexCount()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.ZLexCount("test", "-", "+"));
                Assert.AreEqual("*4\r\n$9\r\nZLEXCOUNT\r\n$4\r\ntest\r\n$1\r\n-\r\n$1\r\n+\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRange()
        {
            string reply1 = "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            string reply2 = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply2, reply2))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRange("test", 0, -1);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0]);
                Assert.AreEqual("test2", response1[1]);
                Assert.AreEqual("*4\r\n$6\r\nZRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n", mock.GetMessage());

                var response2 = redis.ZRange("test", 0, -1, withScores: true);
                Assert.AreEqual(4, response2.Length);
                Assert.AreEqual("test1", response2[0]);
                Assert.AreEqual("1.1", response2[1]);
                Assert.AreEqual("test2", response2[2]);
                Assert.AreEqual("2.2", response2[3]);
                Assert.AreEqual("*5\r\n$6\r\nZRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response3 = redis.ZRangeWithScores("test", 0, -1);
                Assert.AreEqual(2, response3.Length);
                Assert.AreEqual("test1", response3[0].Item1);
                Assert.AreEqual(1.1, response3[0].Item2);
                Assert.AreEqual("test2", response3[1].Item1);
                Assert.AreEqual(2.2, response3[1].Item2);

                Assert.AreEqual("*5\r\n$6\r\nZRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRangeByLex()
        {
            string reply = "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRangeByLex("test", "-", "[c");
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0]);
                Assert.AreEqual("test2", response1[1]);
                Assert.AreEqual("*4\r\n$11\r\nZRANGEBYLEX\r\n$4\r\ntest\r\n$1\r\n-\r\n$2\r\n[c\r\n", mock.GetMessage());

                var response2 = redis.ZRangeByLex("test", "-", "[c", offset: 10, count: 5);
                Assert.AreEqual("*7\r\n$11\r\nZRANGEBYLEX\r\n$4\r\ntest\r\n$1\r\n-\r\n$2\r\n[c\r\n$5\r\nLIMIT\r\n$2\r\n10\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRangeByScore()
        {
            string reply1 = "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            string reply2 = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply1, reply2, reply2, reply2))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRangeByScore("test", 0, 10);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0]);
                Assert.AreEqual("test2", response1[1]);
                Assert.AreEqual("*4\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n", mock.GetMessage());

                var response2 = redis.ZRangeByScore("test", "(0", "(10");
                Assert.AreEqual("*4\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n", mock.GetMessage());

                var response3 = redis.ZRangeByScore("test", 0, 10, withScores: true);
                Assert.AreEqual(4, response3.Length);
                Assert.AreEqual("test1", response3[0]);
                Assert.AreEqual("1.1", response3[1]);
                Assert.AreEqual("test2", response3[2]);
                Assert.AreEqual("2.2", response3[3]);
                Assert.AreEqual("*5\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response4 = redis.ZRangeByScore("test", 0, 10, withScores: true, exclusiveMin: true, exclusiveMax: true);
                Assert.AreEqual("*5\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response5 = redis.ZRangeByScore("test", 0, 10, withScores: true, exclusiveMin: true, exclusiveMax: true, offset: 1, count: 5);
                Assert.AreEqual("*8\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRangeByScoreWithScores()
        {
            string reply = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRangeByScoreWithScores("test", 0, 10);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0].Item1);
                Assert.AreEqual(1.1, response1[0].Item2);
                Assert.AreEqual("test2", response1[1].Item1);
                Assert.AreEqual(2.2, response1[1].Item2);
                Assert.AreEqual("*5\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response2 = redis.ZRangeByScoreWithScores("test", "(0", "(10");
                Assert.AreEqual("*5\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response3 = redis.ZRangeByScoreWithScores("test", 0, 10, exclusiveMin: true, exclusiveMax: true);
                Assert.AreEqual("*5\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response4 = redis.ZRangeByScoreWithScores("test", 0, 10, exclusiveMin: true, exclusiveMax: true, offset: 1, count: 5);
                Assert.AreEqual("*8\r\n$13\r\nZRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRank()
        {
            string reply1 = ":3\r\n";
            string reply2 = "$-1\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply2))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.ZRank("test", "member"));
                Assert.AreEqual("*3\r\n$5\r\nZRANK\r\n$4\r\ntest\r\n$6\r\nmember\r\n", mock.GetMessage());

                Assert.IsNull(redis.ZRank("test", "member"));
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRem()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZRem("test", "m1", "m2"));
                Assert.AreEqual("*4\r\n$4\r\nZREM\r\n$4\r\ntest\r\n$2\r\nm1\r\n$2\r\nm2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRemRangeByLex()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZRemRangeByLex("test", "[a", "[z"));
                Assert.AreEqual("*4\r\n$14\r\nZREMRANGEBYLEX\r\n$4\r\ntest\r\n$2\r\n[a\r\n$2\r\n[z\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRemRangeByRank()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZRemRangeByRank("test", 0, 10));
                Assert.AreEqual("*4\r\n$15\r\nZREMRANGEBYRANK\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRemRangeByScore()
        {
            using (var mock = new FakeRedisSocket(":2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZRemRangeByScore("test", 0, 10));
                Assert.AreEqual("*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRevRange()
        {
            string reply1 = "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            string reply2 = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply2, reply2))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRevRange("test", 0, -1);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0]);
                Assert.AreEqual("test2", response1[1]);
                Assert.AreEqual("*4\r\n$9\r\nZREVRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n", mock.GetMessage());

                var response2 = redis.ZRevRange("test", 0, -1, withScores: true);
                Assert.AreEqual(4, response2.Length);
                Assert.AreEqual("test1", response2[0]);
                Assert.AreEqual("1.1", response2[1]);
                Assert.AreEqual("test2", response2[2]);
                Assert.AreEqual("2.2", response2[3]);
                Assert.AreEqual("*5\r\n$9\r\nZREVRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response3 = redis.ZRevRangeWithScores("test", 0, -1);
                Assert.AreEqual(2, response3.Length);
                Assert.AreEqual("test1", response3[0].Item1);
                Assert.AreEqual(1.1, response3[0].Item2);
                Assert.AreEqual("test2", response3[1].Item1);
                Assert.AreEqual(2.2, response3[1].Item2);
                Assert.AreEqual("*5\r\n$9\r\nZREVRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRevRangeByScore()
        {
            string reply1 = "*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n";
            string reply2 = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply1, reply1, reply2, reply2, reply2))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRevRangeByScore("test", 0, 10);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0]);
                Assert.AreEqual("test2", response1[1]);
                Assert.AreEqual("*4\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n", mock.GetMessage());

                var response2 = redis.ZRevRangeByScore("test", "(0", "(10");
                Assert.AreEqual("*4\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n", mock.GetMessage());

                var response3 = redis.ZRevRangeByScore("test", 0, 10, withScores: true);
                Assert.AreEqual(4, response3.Length);
                Assert.AreEqual("test1", response3[0]);
                Assert.AreEqual("1.1", response3[1]);
                Assert.AreEqual("test2", response3[2]);
                Assert.AreEqual("2.2", response3[3]);
                Assert.AreEqual("*5\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response4 = redis.ZRevRangeByScore("test", 0, 10, withScores: true, exclusiveMin: true, exclusiveMax: true);
                Assert.AreEqual("*5\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response5 = redis.ZRevRangeByScore("test", 0, 10, withScores: true, exclusiveMin: true, exclusiveMax: true, offset: 1, count: 5);
                Assert.AreEqual("*8\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRevRangeByScoreWithScores()
        {
            string reply = "*4\r\n$5\r\ntest1\r\n$3\r\n1.1\r\n$5\r\ntest2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZRevRangeByScoreWithScores("test", 0, 10);
                Assert.AreEqual(2, response1.Length);
                Assert.AreEqual("test1", response1[0].Item1);
                Assert.AreEqual(1.1, response1[0].Item2);
                Assert.AreEqual("test2", response1[1].Item1);
                Assert.AreEqual(2.2, response1[1].Item2);
                Assert.AreEqual("*5\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$1\r\n0\r\n$2\r\n10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response2 = redis.ZRevRangeByScoreWithScores("test", "(0", "(10");
                Assert.AreEqual("*5\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response3 = redis.ZRevRangeByScoreWithScores("test", 0, 10, exclusiveMin: true, exclusiveMax: true);
                Assert.AreEqual("*5\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n", mock.GetMessage());

                var response4 = redis.ZRevRangeByScoreWithScores("test", 0, 10, exclusiveMin: true, exclusiveMax: true, offset: 1, count: 5);
                Assert.AreEqual("*8\r\n$16\r\nZREVRANGEBYSCORE\r\n$4\r\ntest\r\n$2\r\n(0\r\n$3\r\n(10\r\n$10\r\nWITHSCORES\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$1\r\n5\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZRevRank()
        {
            using (var mock = new FakeRedisSocket(":2\r\n", "$-1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZRevRank("test", "test1"));
                Assert.AreEqual("*3\r\n$8\r\nZREVRANK\r\n$4\r\ntest\r\n$5\r\ntest1\r\n", mock.GetMessage());

                Assert.IsNull(redis.ZRevRank("test", "test1"));
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZScan()
        {
            string reply = "*2\r\n$2\r\n23\r\n*4\r\n$7\r\nmember1\r\n$3\r\n1.1\r\n$7\r\nmember2\r\n$3\r\n2.2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response1 = redis.ZScan("test", 0);
                Assert.AreEqual(23, response1.Cursor);
                Assert.AreEqual(2, response1.Items.Length);
                Assert.AreEqual("member1", response1.Items[0].Item1);
                Assert.AreEqual(1.1, response1.Items[0].Item2);
                Assert.AreEqual("*3\r\n$5\r\nZSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n", mock.GetMessage(), "Basic test");

                var response2 = redis.ZScan("test", 0, pattern: "*");
                Assert.AreEqual("*5\r\n$5\r\nZSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n", mock.GetMessage(), "Pattern test");

                var response3 = redis.ZScan("test", 0, count: 5);
                Assert.AreEqual("*5\r\n$5\r\nZSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Count test");

                var response4 = redis.ZScan("test", 0, "*", 5);
                Assert.AreEqual("*7\r\n$5\r\nZSCAN\r\n$4\r\ntest\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n", mock.GetMessage(), "Pattern + Count test");
            }
        }
        [TestMethod, TestCategory("SortedSets")]
        public void TestZScore()
        {
            using (var mock = new FakeRedisSocket("$3\r\n1.1\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(1.1, redis.ZScore("test", "member1"));
                Assert.AreEqual("*3\r\n$6\r\nZSCORE\r\n$4\r\ntest\r\n$7\r\nmember1\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("SortedSets")]
        public void TestZUnionStore()
        {
            string reply = ":2\r\n";
            using (var mock = new FakeRedisSocket(reply, reply, reply, reply, reply))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(2, redis.ZUnionStore("destination", "key1", "key2"));
                Assert.AreEqual("*5\r\n$11\r\nZUNIONSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZUnionStore("destination", weights: new[] { 1D, 2D }, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*8\r\n$11\r\nZUNIONSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZUnionStore("destination", weights: new[] { 1D, 2D }, aggregate: RedisAggregate.Max, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZUNIONSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nMAX\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZUnionStore("destination", weights: new[] { 1D, 2D }, aggregate: RedisAggregate.Min, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZUNIONSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nMIN\r\n", mock.GetMessage());

                Assert.AreEqual(2, redis.ZUnionStore("destination", weights: new[] { 1D, 2D }, aggregate: RedisAggregate.Sum, keys: new[] { "key1", "key2" }));
                Assert.AreEqual("*10\r\n$11\r\nZUNIONSTORE\r\n$11\r\ndestination\r\n$1\r\n2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$7\r\nWEIGHTS\r\n$1\r\n1\r\n$1\r\n2\r\n$9\r\nAGGREGATE\r\n$3\r\nSUM\r\n", mock.GetMessage());
            }
        }
    }
}
