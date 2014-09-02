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
    public class SentinelTests
    {
        [TestMethod, TestCategory("Sentinel")]
        public void PingTest()
        {
            TestSentinel(
                "+PONG\r\n",
                x => x.Ping(),
                x => x.PingAsync(),
                (m, r) =>
                {
                    Assert.AreEqual("PONG", r);
                    Assert.AreEqual(Compile("PING"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void MastersTest()
        {
            TestSentinel(
                "*1\r\n" + Compile(
                    "name", "mymaster",
                    "ip", "127.0.0.1",
                    "port", "6379",
                    "runid", "0e4c05a7b29fdb5dffa054f151bca9ed6a113c38",
                    "flags", "master",
                    "pending-commands", "0",
                    "last-ping-sent", "0",
                    "last-ping-reply", "50",
                    "last-ok-ping-reply", "50",
                    "down-after-milliseconds", "30000",
                    "info-refresh", "1913",
                    "role-reported", "master",
                    "role-reported-time", "1838781",
                    "config-epoch", "0",
                    "num-slaves", "2",
                    "num-other-sentinels", "1",
                    "quorum", "2",
                    "failover-timeout", "180000",
                    "parallel-syncs", "1"),
                x => x.Masters(),
                x => x.MastersAsync(),
                (m, r) =>
                {
                    Assert.AreEqual(1, r.Length);
                    Assert.AreEqual("mymaster", r[0].Name);
                    Assert.AreEqual("127.0.0.1", r[0].Ip);
                    Assert.AreEqual(6379, r[0].Port);
                    Assert.AreEqual("0e4c05a7b29fdb5dffa054f151bca9ed6a113c38", r[0].RunId);
                    Assert.AreEqual(1, r[0].Flags.Length);
                    Assert.AreEqual("master", r[0].Flags[0]);
                    Assert.AreEqual(0, r[0].PendingCommands);
                    Assert.AreEqual(0, r[0].LastPingSent);
                    Assert.AreEqual(50, r[0].LastPingReply);
                    Assert.AreEqual(50, r[0].LastOkPingReply);
                    Assert.AreEqual(30000, r[0].DownAfterMilliseconds);
                    Assert.AreEqual(1913, r[0].InfoRefresh);
                    Assert.AreEqual("master", r[0].RoleReported);
                    Assert.AreEqual(1838781, r[0].RoleReportedTime);
                    Assert.AreEqual(0, r[0].ConfigEpoch);
                    Assert.AreEqual(2, r[0].NumSlaves);
                    Assert.AreEqual(1, r[0].NumOtherSentinels);
                    Assert.AreEqual(2, r[0].Quorum);
                    Assert.AreEqual(180000, r[0].FailoverTimeout);
                    Assert.AreEqual(1, r[0].ParallelSyncs);
                    Assert.AreEqual(Compile("SENTINEL", "masters"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void MasterTest()
        {
            TestSentinel(
                Compile(
                    "name", "mymaster",
                    "ip", "127.0.0.1",
                    "port", "6379",
                    "runid", "0e4c05a7b29fdb5dffa054f151bca9ed6a113c38",
                    "flags", "master",
                    "pending-commands", "0",
                    "last-ping-sent", "0",
                    "last-ping-reply", "50",
                    "last-ok-ping-reply", "50",
                    "down-after-milliseconds", "30000",
                    "info-refresh", "1913",
                    "role-reported", "master",
                    "role-reported-time", "1838781",
                    "config-epoch", "0",
                    "num-slaves", "2",
                    "num-other-sentinels", "1",
                    "quorum", "2",
                    "failover-timeout", "180000",
                    "parallel-syncs", "1"),
                x => x.Master("mymaster"),
                x => x.MasterAsync("mymaster"),
                (m, r) =>
                {
                    Assert.AreEqual("mymaster", r.Name);
                    Assert.AreEqual("127.0.0.1", r.Ip);
                    Assert.AreEqual(6379, r.Port);
                    Assert.AreEqual("0e4c05a7b29fdb5dffa054f151bca9ed6a113c38", r.RunId);
                    Assert.AreEqual(1, r.Flags.Length);
                    Assert.AreEqual("master", r.Flags[0]);
                    Assert.AreEqual(0, r.PendingCommands);
                    Assert.AreEqual(0, r.LastPingSent);
                    Assert.AreEqual(50, r.LastPingReply);
                    Assert.AreEqual(50, r.LastOkPingReply);
                    Assert.AreEqual(30000, r.DownAfterMilliseconds);
                    Assert.AreEqual(1913, r.InfoRefresh);
                    Assert.AreEqual("master", r.RoleReported);
                    Assert.AreEqual(1838781, r.RoleReportedTime);
                    Assert.AreEqual(0, r.ConfigEpoch);
                    Assert.AreEqual(2, r.NumSlaves);
                    Assert.AreEqual(1, r.NumOtherSentinels);
                    Assert.AreEqual(2, r.Quorum);
                    Assert.AreEqual(180000, r.FailoverTimeout);
                    Assert.AreEqual(1, r.ParallelSyncs);
                    Assert.AreEqual(Compile("SENTINEL", "master", "mymaster"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void SlavesTest()
        {
            TestSentinel(
                "*1\r\n" + Compile(
                    "name", "127.0.0.1:7379",
                    "ip", "127.0.0.1",
                    "port", "7379",
                    "runid", "0e4c05a7b29fdb5dffa054f151bca9ed6a113c38",
                    "flags", "slave",
                    "pending-commands", "0",
                    "last-ping-sent", "0",
                    "last-ok-ping-reply", "50",
                    "last-ping-reply", "50",
                    "down-after-milliseconds", "1000",
                    "info-refresh", "9036",
                    "role-reported", "slave",
                    "role-reported-time", "4396444",
                    "master-link-down-time", "0",
                    "master-link-status", "ok",
                    "master-host", "127.0.0.1",
                    "master-port", "6379",
                    "slave-priority", "100",
                    "slave-repl-offset", "509813"),
                x => x.Slaves("mymaster"),
                x => x.SlavesAsync("mymaster"),
                (m, r) =>
                {
                    Assert.AreEqual(1, r.Length);
                    Assert.AreEqual("127.0.0.1:7379", r[0].Name);
                    Assert.AreEqual("127.0.0.1", r[0].Ip);
                    Assert.AreEqual(7379, r[0].Port);
                    Assert.AreEqual("0e4c05a7b29fdb5dffa054f151bca9ed6a113c38", r[0].RunId);
                    Assert.AreEqual(1, r[0].Flags.Length);
                    Assert.AreEqual("slave", r[0].Flags[0]);
                    Assert.AreEqual(0, r[0].PendingCommands);
                    Assert.AreEqual(50, r[0].LastPingReply);
                    Assert.AreEqual(50, r[0].LastOkPingReply);
                    Assert.AreEqual(1000, r[0].DownAfterMilliseconds);
                    Assert.AreEqual(9036, r[0].InfoRefresh);
                    Assert.AreEqual("slave", r[0].RoleReported);
                    Assert.AreEqual(4396444, r[0].RoleReportedTime);
                    Assert.AreEqual(0, r[0].MasterLinkDownTime);
                    Assert.AreEqual("ok", r[0].MasterLinkStatus);
                    Assert.AreEqual("127.0.0.1", r[0].MasterHost);
                    Assert.AreEqual(6379, r[0].MasterPort);
                    Assert.AreEqual(100, r[0].SlavePriority);
                    Assert.AreEqual(509813, r[0].SlaveReplOffset);
                    Assert.AreEqual(Compile("SENTINEL", "slaves", "mymaster"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void GetMasterAddrByNameTest()
        {
            TestSentinel(
                Compile("127.0.0.1", "6379"),
                x => x.GetMasterAddrByName("mymaster"),
                x => x.GetMasterAddrByNameAsync("mymaster"),
                (m, r) =>
                {
                    Assert.AreEqual("127.0.0.1", r.Item1);
                    Assert.AreEqual(6379, r.Item2);
                    Assert.AreEqual(Compile("SENTINEL", "get-master-addr-by-name", "mymaster"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void IsMasterDownByAddrTest()
        {
            TestSentinel(
                Compile(0, "*", 0),
                x => x.IsMasterDownByAddr("127.0.0.1", 6379, 123, "abc"),
                x => x.IsMasterDownByAddrAsync("127.0.0.1", 6379, 123, "abc"),
                (m, r) =>
                {
                    Assert.AreEqual(0, r.DownState);
                    Assert.AreEqual("*", r.Leader);
                    Assert.AreEqual(0, r.VoteEpoch);
                    Assert.AreEqual(Compile("SENTINEL", "is-master-down-by-addr", "127.0.0.1", "6379", "123", "abc"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void ResetTest()
        {
            TestSentinel(
                ":10\r\n",
                x => x.Reset("pattern*"),
                x => x.ResetAsync("pattern*"),
                (m, r) =>
                {
                    Assert.AreEqual(10, r);
                    Assert.AreEqual(Compile("SENTINEL", "reset", "pattern*"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void FailoverTest()
        {
            TestSentinel(
                "+OK\r\n",
                x => x.Failover("mymaster"),
                x => x.FailoverAsync("mymaster"),
                (m, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual(Compile("SENTINEL", "failover", "mymaster"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void MonitorTest()
        {
            TestSentinel(
                "+OK\r\n",
                x => x.Monitor("mymaster", 6379, 2),
                x => x.MonitorAsync("mymaster", 6379, 2),
                (m, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual(Compile("SENTINEL", "MONITOR", "mymaster", "6379", "2"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void RemoveTest()
        {
            TestSentinel(
                "+OK\r\n",
                x => x.Remove("mymaster"),
                x => x.RemoveAsync("mymaster"),
                (m, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual(Compile("SENTINEL", "REMOVE", "mymaster"), m.GetMessage());
                });
        }

        [TestMethod, TestCategory("Sentinel")]
        public void SetTest()
        {
            TestSentinel(
                "+OK\r\n",
                x => x.Set("mymaster", "my-option", "my-value"),
                x => x.SetAsync("mymaster", "my-option", "my-value"),
                (m, r) =>
                {
                    Assert.AreEqual("OK", r);
                    Assert.AreEqual(Compile("SENTINEL", "SET", "mymaster", "my-option", "my-value"), m.GetMessage());
                });
        }



        static void TestSentinel<T>(string reply, Func<RedisSentinelClient, T> syncFunc, Func<RedisSentinelClient, Task<T>> asyncFunc, Action<FakeRedisSocket, T> test)
        {
            using (var mock = new FakeRedisSocket(reply, reply))
            using (var sentinel = new RedisSentinelClient(mock, new DnsEndPoint("fakehost", 9999)))
            {
                var sync_result = syncFunc(sentinel);
                var async_result = asyncFunc(sentinel);
                test(mock, sync_result);
                test(mock, async_result.Result);
            }
        }

        static string Compile(params string[] parts)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('*').Append(parts.Length).Append("\r\n");
            for (int i = 0; i < parts.Length; i++)
                sb.Append('$')
                    .Append(parts[i].Length).Append("\r\n")
                    .Append(parts[i]).Append("\r\n");
            return sb.ToString();
        }

        static string Compile(params object[] parts)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append('*').Append(parts.Length).Append("\r\n");
            for (int i = 0; i < parts.Length; i++)
            {
                if (parts[i] is String)
                    sb.Append('$')
                        .Append((parts[i] as String).Length).Append("\r\n")
                        .Append((parts[i] as String)).Append("\r\n");
                else if ((parts[i] is Int64) || (parts[i] is Int32))
                    sb.Append(':')
                        .Append(parts[i]).Append("\r\n");
                else
                    throw new Exception();
            }
            return sb.ToString();
        }
    }
}
