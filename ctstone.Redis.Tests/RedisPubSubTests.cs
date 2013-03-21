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

            using (var redisConsumer = new RedisClient(Host, Port, 0))
            using (var redisPublisher = new RedisClient(Host, Port, 0))
            {
                redisConsumer.Auth(Password);
                redisPublisher.Auth(Password);

                redisConsumer.SubscriptionReceived += (o, e) =>
                {
                    message_count++;
                    last_message = e.Message.Body;
                };
                redisConsumer.SubscriptionChanged += (o, e) =>
                {
                    change_count++;
                };
                Task consumer_task = Task.Factory.StartNew(() =>
                {
                    redisConsumer.Subscribe("test");
                });
                while (change_count != 1) // wait for subscribe
                    Thread.Sleep(10);

                Assert.AreEqual(1, redisPublisher.Publish("test", "hello world"), "First publish");
                Assert.AreEqual(0, redisPublisher.Publish("junk", "nothing"), "Junk publish");
                Assert.AreEqual(1, redisPublisher.Publish("test", "hello again"), "Second publish");
                redisConsumer.Subscribe("test2");
                while (change_count != 2) // wait for subscribe
                    Thread.Sleep(10);
                Assert.AreEqual(1, redisPublisher.Publish("test2", "a new channel"), "New channel publish");

                redisConsumer.Unsubscribe("test");
                redisConsumer.Unsubscribe();
                consumer_task.Wait();

                Assert.AreEqual(3, message_count);
                Assert.AreEqual(4, change_count);
                Assert.AreEqual("a new channel", last_message);
            }
        }

        [TestMethod, TestCategory("PubSub")]
        public void TestPSubscribe()
        {
            int change_count = 0;
            int message_count = 0;
            string last_message = String.Empty;

            using (var redisConsumer = new RedisClient(Host, Port, 0))
            using (var redisPublisher = new RedisClient(Host, Port, 0))
            {
                redisConsumer.Auth(Password);
                redisPublisher.Auth(Password);

                redisConsumer.SubscriptionReceived += (o, e) =>
                {
                    message_count++;
                    last_message = e.Message.Body;
                };
                redisConsumer.SubscriptionChanged += (o, e) =>
                {
                    change_count++;
                };
                Task consumer_task = Task.Factory.StartNew(() =>
                {
                    redisConsumer.PSubscribe("t*");
                });
                while (change_count != 1) // wait for psubscribe
                    Thread.Sleep(10);

                Assert.AreEqual(1, redisPublisher.Publish("test", "hello world"), "First publish");
                Assert.AreEqual(0, redisPublisher.Publish("junk", "nothing"), "Junk publish");
                Assert.AreEqual(1, redisPublisher.Publish("test2", "hello again"), "Second publish");
                redisConsumer.PSubscribe("c*");
                while (change_count != 2) // wait for psubscribe
                    Thread.Sleep(10);
                Assert.AreEqual(1, redisPublisher.Publish("channel1", "something new"), "New channel publish");

                redisConsumer.PUnsubscribe("t*");
                redisConsumer.PUnsubscribe();
                consumer_task.Wait();

                Assert.AreEqual(3, message_count);
                Assert.AreEqual(4, change_count);
                Assert.AreEqual("something new", last_message);
            }
        }
    }
}
