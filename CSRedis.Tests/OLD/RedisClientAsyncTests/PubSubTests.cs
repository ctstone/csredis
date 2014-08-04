/*using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ctstone.Redis.Tests.RedisClientTests;
using System.Threading;
using System.Threading.Tasks;

namespace ctstone.Redis.Tests.RedisClientAsyncTests
{
    [TestClass]
    public class PubSubTests : RedisTestBase
    {
        [TestMethod, TestCategory("PubSub"), TestCategory("RedisClientAsync")]
        public void TestSubscribe() 
        {
            using (var channel = Async.SubscriptionChannel)
            {
                int change_count = 0;
                int message_count = 0;
                int callback_count = 0;
                channel.SubscriptionChanged += (s, a) =>
                {
                    Assert.AreEqual("test1", a.Response.Channel);
                    Assert.IsTrue(a.Response.Type == RedisSubscriptionResponseType.Subscribe || a.Response.Type == RedisSubscriptionResponseType.Unsubscribe);
                    if (a.Response.Type == RedisSubscriptionResponseType.Subscribe)
                        Assert.AreEqual(1, a.Response.Count);
                    else if (a.Response.Type == RedisSubscriptionResponseType.Unsubscribe)
                        Assert.AreEqual(0, a.Response.Count);
                    change_count++;
                };
                channel.SubscriptionReceived += (s, a) =>
                {
                    Assert.AreEqual("test1", a.Message.Channel);
                    Assert.AreEqual("hello world", a.Message.Body);
                    Assert.AreEqual(RedisSubscriptionResponseType.Message, a.Message.Type);
                    message_count++;
                };

                channel.Subscribe(m =>
                {
                    Assert.AreEqual("test1", m.Channel);
                    Assert.AreEqual("hello world", m.Body);
                    Assert.AreEqual(RedisSubscriptionResponseType.Message, m.Type);
                    callback_count++;
                }, "test1");

                while (change_count == 0)
                    Thread.Sleep(10); // waiting for subscribe to complete

                Assert.AreEqual(1, Async.Wait(r => r.Publish("test1", "hello world")));

                while (message_count == 0)
                    Thread.Sleep(10); // wait for message received

                channel.Unsubscribe("test1");
                
                while (change_count == 1)
                    Thread.Sleep(10); // waiting for unsubscribe to complete

                channel.Dispose();

                Assert.AreEqual(2, change_count);
                Assert.AreEqual(1, message_count);
                Assert.AreEqual(1, callback_count);
            }
        }

        [TestMethod, TestCategory("PubSub"), TestCategory("RedisClientAsync")]
        public void TestSubscribeTasks()
        {
            int change_count = 0;
            int message_count = 0;
            Async.SubscriptionChannel.SubscriptionChanged += (s, a) =>
            {
                change_count++;
            };
            Async.SubscriptionChannel.SubscriptionReceived += (s, a) =>
            {
                message_count++;
            };

            Task[] tasks = new Task[10];
            for (int i_ = 0; i_ < tasks.Length; i_++)
            {
                int i = i_;
                tasks[i_] = Task.Factory.StartNew(() =>
                {
                    int task_message_count = 0;
                    Async.SubscriptionChannel.Subscribe(x => 
                    { 
                        Assert.AreEqual("test" + i, x.Channel);
                        Assert.AreEqual("message" + i, x.Body);
                        task_message_count++;
                    }, "test" + i);
                    
                    while (task_message_count == 0)
                        Thread.Sleep(10); // wait for message 

                    Assert.AreEqual(1, task_message_count);
                    Async.SubscriptionChannel.Unsubscribe("test" + i);
                });
            }

            while (change_count != tasks.Length)
                Thread.Sleep(10); // wait for all subscriptions

            for (int i = 0; i < tasks.Length; i++)
                Async.Publish("test" + i, "message" + i);

            while (change_count != tasks.Length * 2)
                Thread.Sleep(10); // wait for all unsubscribes

            Async.CloseSubscriptionChannel();

            Assert.AreEqual(tasks.Length * 2, change_count);
            Assert.AreEqual(tasks.Length, message_count);
        }
    }
}
*/