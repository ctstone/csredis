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
    public class PubSubTests
    {
        /*[TestMethod, TestCategory("PubSub")]
        public void PSubscriptionTest()
        {
            using (var mock = new FakeRedisSocket(true,
                "*3\r\n$10\r\npsubscribe\r\n$2\r\nf*\r\n:1\r\n"
                    + "*3\r\n$10\r\npsubscribe\r\n$2\r\ns*\r\n:2\r\n"
                    + "*4\r\n$8\r\npmessage\r\n$2\r\nf*\r\n$5\r\nfirst\r\n$5\r\nHello\r\n",
                "*3\r\n$12\r\npunsubscribe\r\n$2\r\ns*\r\n:1\r\n*3\r\n$12\r\npunsubscribe\r\n$2\r\nf*\r\n:0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var changes = new List<RedisSubscriptionChannel>();
                var messages = new List<RedisSubscriptionMessage>();
                redis.SubscriptionChanged += (s,a) => changes.Add(a.Response);
                redis.SubscriptionReceived += (s, a) => messages.Add(a.Message);
                Task.Delay(500)
                    .ContinueWith(t => redis.PUnsubscribe())
                    .ContinueWith(t =>
                    {
                        Assert.AreEqual(4, changes.Count);
                        Assert.AreEqual("f*", changes[0].Pattern);
                        Assert.AreEqual(1, changes[0].Count);
                        Assert.IsNull(changes[0].Channel);
                        Assert.AreEqual("psubscribe", changes[0].Type);

                        Assert.AreEqual("s*", changes[1].Pattern);
                        Assert.AreEqual(2, changes[1].Count);
                        Assert.IsNull(changes[1].Channel);
                        Assert.AreEqual("psubscribe", changes[1].Type);

                        Assert.AreEqual("s*", changes[2].Pattern);
                        Assert.AreEqual(1, changes[2].Count);
                        Assert.IsNull(changes[2].Channel);
                        Assert.AreEqual("punsubscribe", changes[2].Type);

                        Assert.AreEqual("f*", changes[3].Pattern);
                        Assert.AreEqual(0, changes[3].Count);
                        Assert.IsNull(changes[3].Channel);
                        Assert.AreEqual("punsubscribe", changes[3].Type);

                        Assert.AreEqual(1, messages.Count);
                        Assert.AreEqual("f*", messages[0].Pattern);
                        Assert.AreEqual("first", messages[0].Channel);
                        Assert.AreEqual("Hello", messages[0].Body);
                        Assert.AreEqual("pmessage", messages[0].Type);
                    });
                redis.PSubscribe("f*", "s*");
            }
        }*/

        [TestMethod, TestCategory("PubSub")]
        public void PublishTest()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.Publish("test", "message"));
                Assert.AreEqual("*3\r\n$7\r\nPUBLISH\r\n$4\r\ntest\r\n$7\r\nmessage\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("PubSub")]
        public void PubSubChannelsTest()
        {
            using (var mock = new FakeRedisSocket("*2\r\n$5\r\ntest1\r\n$5\r\ntest2\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.PubSubChannels("pattern");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0]);
                Assert.AreEqual("test2", response[1]);
                Assert.AreEqual("*3\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n$7\r\npattern\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("PubSub")]
        public void PubSubNumSubTest()
        {
            using (var mock = new FakeRedisSocket("*4\r\n$5\r\ntest1\r\n:1\r\n$5\r\ntest2\r\n:5\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var response = redis.PubSubNumSub("channel1", "channel2");
                Assert.AreEqual(2, response.Length);
                Assert.AreEqual("test1", response[0].Item1);
                Assert.AreEqual(1, response[0].Item2);
                Assert.AreEqual("test2", response[1].Item1);
                Assert.AreEqual(5, response[1].Item2);
                Assert.AreEqual("*4\r\n$6\r\nPUBSUB\r\n$6\r\nNUMSUB\r\n$8\r\nchannel1\r\n$8\r\nchannel2\r\n", mock.GetMessage());
            }
        }

        [TestMethod, TestCategory("PubSub")]
        public void PubSubNumPatTest()
        {
            using (var mock = new FakeRedisSocket(":3\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                Assert.AreEqual(3, redis.PubSubNumPat());
                Assert.AreEqual("*2\r\n$6\r\nPUBSUB\r\n$6\r\nNUMPAT\r\n", mock.GetMessage());
            }
        }

        /*[TestMethod, TestCategory("PubSub")]
        public void SubscriptionTest()
        {
            using (var mock = new FakeRedisSocket(true,
                "*3\r\n$9\r\nsubscribe\r\n$5\r\nfirst\r\n:1\r\n"
                    + "*3\r\n$9\r\nsubscribe\r\n$6\r\nsecond\r\n:2\r\n"
                    + "*3\r\n$7\r\nmessage\r\n$5\r\nfirst\r\n$5\r\nHello\r\n",
                "*3\r\n$11\r\nunsubscribe\r\n$6\r\nsecond\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$5\r\nfirst\r\n:0\r\n"))
            using (var redis = new RedisClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var changes = new List<RedisSubscriptionChannel>();
                var messages = new List<RedisSubscriptionMessage>();
                redis.SubscriptionChanged += (s, a) => changes.Add(a.Response);
                redis.SubscriptionReceived += (s, a) => messages.Add(a.Message);
                Task.Delay(500)
                    .ContinueWith(t => redis.Unsubscribe())
                    .ContinueWith(t =>
                {
                    Assert.AreEqual(4, changes.Count);
                    Assert.AreEqual("first", changes[0].Channel);
                    Assert.AreEqual(1, changes[0].Count);
                    Assert.IsNull(changes[0].Pattern);
                    Assert.AreEqual("subscribe", changes[0].Type);

                    Assert.AreEqual("second", changes[1].Channel);
                    Assert.AreEqual(2, changes[1].Count);
                    Assert.IsNull(changes[1].Pattern);
                    Assert.AreEqual("subscribe", changes[1].Type);

                    Assert.AreEqual("second", changes[2].Channel);
                    Assert.AreEqual(1, changes[2].Count);
                    Assert.IsNull(changes[2].Pattern);
                    Assert.AreEqual("unsubscribe", changes[2].Type);

                    Assert.AreEqual("first", changes[3].Channel);
                    Assert.AreEqual(0, changes[3].Count);
                    Assert.IsNull(changes[3].Pattern);
                    Assert.AreEqual("unsubscribe", changes[3].Type);

                    Assert.AreEqual(1, messages.Count);
                    Assert.IsNull(messages[0].Pattern);
                    Assert.AreEqual("first", messages[0].Channel);
                    Assert.AreEqual("Hello", messages[0].Body);
                    Assert.AreEqual("message", messages[0].Type);
                });
                redis.Subscribe("first", "second");
            }
        }*/
    }
}
