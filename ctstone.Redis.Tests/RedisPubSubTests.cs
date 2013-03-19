using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis;
using System.Threading.Tasks;
using System.Threading;

namespace ctstone.Redis.Tests
{
    [TestClass]
    public class RedisPubSubTests : RedisTestBase
    {
        [TestMethod, TestCategory("PubSub")]
        public void TestSubscribe()
        {
            int change_count = 0;
            int message_count = 0;
            string last_message = String.Empty;
            _redis.SubscriptionReceived += (o, e) =>
            {
                message_count++;
                last_message = e.Message.Message;
            };
            _redis.SubscriptionChanged += (o, e) =>
            {
                change_count++;
            };

            Task consumer = Task.Factory.StartNew(() =>
            {
                _redis.Subscribe("test");
            });
            while (consumer.Status != TaskStatus.Running)
                Thread.Sleep(10);

            using (var publisher = new RedisClient(Host, Port, 0))
            {
                publisher.Auth(Password);
                Assert.AreEqual(1, publisher.Publish("test", "hello world"));
                Assert.AreEqual(0, publisher.Publish("junk", "nothing"));
                Assert.AreEqual(1, publisher.Publish("test", "hello again"));
                _redis.Subscribe("test2");
                Assert.AreEqual(1, publisher.Publish("test2", "a new channel"));
            }

            _redis.Unsubscribe("test");
            _redis.Unsubscribe();
            consumer.Wait();

            Assert.AreEqual(3, message_count);
            Assert.AreEqual(4, change_count);
            Assert.AreEqual("a new channel", last_message);
        }

        [TestMethod, TestCategory("PubSub")]
        public void TestPSubscribe()
        {
            int change_count = 0;
            int message_count = 0;
            string last_message = String.Empty;
            _redis.SubscriptionReceived += (o, e) =>
            {
                message_count++;
                last_message = e.Message.Message;
            };
            _redis.SubscriptionChanged += (o, e) =>
            {
                change_count++;
            };

            Task consumer = Task.Factory.StartNew(() =>
            {
                _redis.PSubscribe("t*");
            });
            while (consumer.Status != TaskStatus.Running)
                Thread.Sleep(10);

            using (var publisher = new RedisClient(Host, Port, 0))
            {
                publisher.Auth(Password);
                Assert.AreEqual(1, publisher.Publish("test", "hello world"));
                Assert.AreEqual(0, publisher.Publish("junk", "nothing"));
                Assert.AreEqual(1, publisher.Publish("test2", "a new channel"));
                _redis.PSubscribe("c*");
                Assert.AreEqual(1, publisher.Publish("channel1", "something new"));
            }

            _redis.PUnsubscribe("t*");
            _redis.PUnsubscribe();
            consumer.Wait();

            Assert.AreEqual(3, message_count);
            Assert.AreEqual(4, change_count);
            Assert.AreEqual("something new", last_message);
        }
    }
}
