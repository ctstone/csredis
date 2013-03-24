using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;

namespace ctstone.Redis.Tests
{
    [TestClass]
    public class SyncTests : RedisTestBase
    {
        [TestMethod]
        public void TestStreaming()
        {
            string bytes_512 = "stuff stuff stuff. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse semper porta tellus, vitae sagittis arcu iaculis vitae. Sed sit amet pulvinar ipsum. Cras et orci est. Phasellus scelerisque dictum ligula, a volutpat sem rhoncus vitae. Quisque vel lobortis est. Maecenas interdum diam in magna adipiscing porttitor nec sit amet ipsum. Phasellus ornare vestibulum porta. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam a tellus non neque mollis accumsan. In sed.";
            string bytes_513 = "xstuff stuff stuff. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse semper porta tellus, vitae sagittis arcu iaculis vitae. Sed sit amet pulvinar ipsum. Cras et orci est. Phasellus scelerisque dictum ligula, a volutpat sem rhoncus vitae. Quisque vel lobortis est. Maecenas interdum diam in magna adipiscing porttitor nec sit amet ipsum. Phasellus ornare vestibulum porta. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Aliquam a tellus non neque mollis accumsan. In sed.";

            using (new RedisTestKeys(_redis, "test1"))
            {
                _redis.Set("test1", bytes_512);
                using (var ms = new MemoryStream())
                {
                    _redis.StreamTo(ms, 64, x => x.Get("test1"));
                    Assert.AreEqual(bytes_512, Encoding.UTF8.GetString(ms.ToArray()), "Even-multiple streamed read failed");
                }

                _redis.Set("test1", bytes_513);
                using (var ms = new MemoryStream())
                {
                    _redis.StreamTo(ms, 64, x => x.Get("test1"));
                    Assert.AreEqual(bytes_513, Encoding.UTF8.GetString(ms.ToArray()), "Odd-multiple streamed read failed");
                }
            }
        }
    }
}
