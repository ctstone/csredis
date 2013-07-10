using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;
using System.Threading;

namespace ctstone.Redis.Tests.RedisClientTests
{
    [TestClass]
    public class GeneralTests : RedisTestBase
    {
        [TestMethod, TestCategory("RedisClient"), TestCategory("Pipeline")]
        public void TestPipeline()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                bool transaction_started = false;
                bool transaction_queued = false;

                Redis.TransactionStarted += (s, e) =>
                {
                    Assert.AreEqual("OK", e.Status);
                    transaction_started = true;
                };
                Redis.TransactionQueued += (s, e) =>
                {
                    Assert.AreEqual("QUEUED", e.Status);
                    transaction_queued = true;

                };

                Redis.StartPipe();
                Assert.AreEqual(default(DateTime), Redis.Time());
                Assert.IsNull(Redis.Set("test1", "value"));
                var result = Redis.EndPipe();
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result.Length);
                Assert.AreNotEqual(default(DateTime), result[0]);
                Assert.AreEqual("OK", result[1]);

                Assert.IsFalse(transaction_started);
                Assert.IsFalse(transaction_queued);
            }
        }

        [TestMethod, TestCategory("RedisClient"), TestCategory("Pipeline")]
        public void TestPipelineTransactionExec()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                bool transaction_started = false;
                bool transaction_queued = false;

                Redis.TransactionStarted += (s, e) =>
                {
                    Assert.AreEqual("OK", e.Status);
                    transaction_started = true;
                };
                Redis.TransactionQueued += (s, e) =>
                {
                    Assert.AreEqual("QUEUED", e.Status);
                    transaction_queued = true;

                };

                Redis.StartPipe(asTransaction: true);
                Assert.AreEqual(default(DateTime), Redis.Time());
                Assert.IsNull(Redis.Set("test1", "value"));
                var result = Redis.EndPipe();
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result.Length);
                Assert.AreNotEqual(default(DateTime), result[0]);
                Assert.AreEqual("OK", result[1]);

                Thread.Sleep(100);
                Assert.IsTrue(transaction_started);
                Assert.IsTrue(transaction_queued);
            }
        }

        [TestMethod, TestCategory("RedisClient"), TestCategory("Pipeline")]
        public void TestPipelineTransactionDiscard()
        {
            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.StartPipe(asTransaction: true);
                Assert.AreEqual(default(DateTime), Redis.Time());
                Assert.IsNull(Redis.Discard());
                Assert.IsNull(Redis.Set("test1", "value"));
                var result = Redis.EndPipe();
                Assert.IsNotNull(result);
                Assert.AreEqual(2, result.Length);
                Assert.AreEqual("OK", result[0]);
                Assert.AreEqual("OK", result[1]);
            }
        }

        [TestMethod, TestCategory("RedisClient")]
        public void TestStreaming()
        {
            string data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse semper porta tellus, vitae sagittis arcu iaculis vitae. Sed sit amet pulvinar ipsum. Cras et orci est. Phasellus scelerisque dictum ligula, a volutpat sem rhoncus vitae. Quisque vel lobortis est. Maecenas interdum diam in magna adipiscing porttitor nec sit amet ipsum. Phasellus ornare vestibulum porta. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam a tellus non neque mollis accumsan. In sed.";

            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Set("test1", data);

                using (var ms = new MemoryStream())
                {
                    Redis.StreamTo(ms, 64, x => x.Get("test1"));
                    Assert.AreEqual(data, Encoding.UTF8.GetString(ms.ToArray()), "Even-multiple-buffer streamed read failed");
                }

                using (var ms = new MemoryStream())
                {
                    Redis.StreamTo(ms, 10, x => x.Get("test1"));
                    Assert.AreEqual(data, Encoding.UTF8.GetString(ms.ToArray()), "Odd-multiple-buffer streamed read failed");
                }

                using (var ms = new MemoryStream())
                {
                    Redis.StreamTo(ms, 512, x => x.Get("test1"));
                    Assert.AreEqual(data, Encoding.UTF8.GetString(ms.ToArray()), "Exact-buffer streamed read failed");
                }

                using (var ms = new MemoryStream())
                {
                    Redis.StreamTo(ms, 1024, x => x.Get("test1"));
                    Assert.AreEqual(data, Encoding.UTF8.GetString(ms.ToArray()), "2x-buffer streamed read failed");
                }
            }
        }

        [TestMethod, TestCategory("RedisClient")]
        public void TestBuffering()
        {
            string data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse semper porta tellus, vitae sagittis arcu iaculis vitae. Sed sit amet pulvinar ipsum. Cras et orci est. Phasellus scelerisque dictum ligula, a volutpat sem rhoncus vitae. Quisque vel lobortis est. Maecenas interdum diam in magna adipiscing porttitor nec sit amet ipsum. Phasellus ornare vestibulum porta. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam a tellus non neque mollis accumsan. In sed.";

            using (new RedisTestKeys(Redis, "test1"))
            {
                Redis.Set("test1", data);
                {
                    Redis.BufferFor(x => x.Get("test1"));
                    byte[] buffer = new byte[64];
                    int bytes_read;
                    StringBuilder sb = new StringBuilder();
                    while ((bytes_read = Redis.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        sb.Append(Encoding.UTF8.GetString(buffer, 0, bytes_read));
                    }
                    Assert.AreEqual(data, sb.ToString(), "Even-multiple-buffer failed");
                }
                {
                    Redis.BufferFor(x => x.Get("test1"));
                    byte[] buffer = new byte[10];
                    int bytes_read;
                    StringBuilder sb = new StringBuilder();
                    while ((bytes_read = Redis.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        sb.Append(Encoding.UTF8.GetString(buffer, 0, bytes_read));
                    }
                    Assert.AreEqual(data, sb.ToString(), "Odd-multiple-buffer failed");
                }

                Redis.Set("test1", data);
                {
                    Redis.BufferFor(x => x.Get("test1"));
                    byte[] buffer = new byte[64];
                    int bytes_read;
                    StringBuilder sb = new StringBuilder();
                    while ((bytes_read = Redis.Read(buffer, 5, buffer.Length / 2)) > 0)
                    {
                        sb.Append(Encoding.UTF8.GetString(buffer, 5, bytes_read));
                    }
                    Assert.AreEqual(data, sb.ToString(), "Weird offset/count buffer failed");
                }
                Redis.Set("test1", data);
                {
                    Redis.BufferFor(x => x.Get("test1"));
                    byte[] buffer = new byte[1024];
                    int bytes_read;
                    StringBuilder sb = new StringBuilder();
                    while ((bytes_read = Redis.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        sb.Append(Encoding.UTF8.GetString(buffer, 0, bytes_read));
                    }
                    Assert.AreEqual(data, sb.ToString(), "2x-buffer failed");
                }
            }
        }
    }
}
