using ctstone.Redis.Tests.RedisClientTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace ctstone.Redis.Tests.RedisClientAsyncTests
{
    [TestClass]
    public class GeneralTests : RedisTestBase
    {
        [TestMethod, TestCategory("RedisClientAsync")]
        public void TestWait()
        {
            using (new RedisTestKeys(_async, "test1"))
            {
                Assert.AreEqual("OK", _async.Wait(x => x.Set("test1", "hello world")));
                Assert.AreEqual("hello world", _async.Wait(x => x.Get("test1")));
            }
        }

        [TestMethod, TestCategory("RedisClientAsync")]
        public void TestConcurrency()
        {
            int task_count = 10;
            int incr_count = 5000;

            Task[] tasks = new Task[task_count];
            string[] keys = new string[tasks.Length];
            for (int i = 0; i < keys.Length; i++)
                keys[i] = "test" + i;

            using (new RedisTestKeys(_async, keys))
            {
                for (int i = 0; i < tasks.Length; i++)
                {
                    int task_id = i;
                    tasks[i] = Task.Factory.StartNew(() =>
                    {
                        for (int j = 0; j < incr_count + task_id; j++)
                        {
                            int expected = j + 1;
                            _async.Incr(keys[task_id]).ContinueWith(t =>
                            {
                                Assert.AreEqual(expected, t.Result);
                            });

                            _async.Echo(keys[task_id]).ContinueWith(t =>
                            {
                                Assert.AreEqual(keys[task_id], t.Result);
                            });
                        }
                    });
                }

                Task.WaitAll(tasks);

                for (int i = 0; i < tasks.Length; i++)
                {
                    Assert.AreEqual((incr_count + i).ToString(), _async.Wait(x => x.Get(keys[i])));
                }

                Task.WaitAll(tasks);
            }
        }
    }
}
