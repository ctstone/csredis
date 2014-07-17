
using System;
using System.Runtime.Serialization;
namespace ctstone.Redis
{
    /// <summary>
    /// Sub-command used by Redis OBJECT command
    /// </summary>
    public enum RedisObjectSubCommand
    {
        /// <summary>
        /// Return the number of references of the value associated with the specified key
        /// </summary>
        RefCount,

        /// <summary>
        /// Return the number of seconds since the object stored at the specified key is idle
        /// </summary>
        IdleTime,
    };

    /// <summary>
    /// Sort direction used by Redis SORT command
    /// </summary>
    public enum RedisSortDir
    {
        /// <summary>
        /// Sort ascending (a-z)
        /// </summary>
        Asc,

        /// <summary>
        /// Sort descending (z-a)
        /// </summary>
        Desc,
    }

    /// <summary>
    /// Insert position used by Redis LINSERT command
    /// </summary>
    public enum RedisInsert
    {
        /// <summary>
        /// Insert before pivot element
        /// </summary>
        Before,

        /// <summary>
        /// Insert after pivot element
        /// </summary>
        After,
    }

    /// <summary>
    /// Operation used by Redis BITOP command
    /// </summary>
    public enum RedisBitOp
    {
        /// <summary>
        /// Bitwise AND
        /// </summary>
        And,

        /// <summary>
        /// Bitwise OR
        /// </summary>
        Or,

        /// <summary>
        /// Bitwise EXCLUSIVE-OR
        /// </summary>
        XOr,

        /// <summary>
        /// Bitwise NOT
        /// </summary>
        Not,
    }

    /// <summary>
    /// Aggregation function used by Reids set operations
    /// </summary>
    public enum RedisAggregate
    {
        /// <summary>
        /// Aggregate SUM
        /// </summary>
        Sum,

        /// <summary>
        /// Aggregate MIN
        /// </summary>
        Min,

        /// <summary>
        /// Aggregate MAX
        /// </summary>
        Max,
    }

    /// <summary>
    /// Redis unified message prefix
    /// </summary>
    public enum RedisMessage
    {
        /// <summary>
        /// Error message
        /// </summary>
        Error = '-',

        /// <summary>
        /// Status message
        /// </summary>
        Status = '+',

        /// <summary>
        /// Bulk message
        /// </summary>
        Bulk = '$',

        /// <summary>
        /// Multi bulk message
        /// </summary>
        MultiBulk = '*',

        /// <summary>
        /// Int message
        /// </summary>
        Int = ':',
    }

    /// <summary>
    /// Redis subscription response type
    /// </summary>
    public enum RedisSubscriptionResponseType
    {
        /// <summary>
        /// Channel subscribed
        /// </summary>
        Subscribe,

        /// <summary>
        /// Message published
        /// </summary>
        Message,

        /// <summary>
        /// Channel unsubscribed
        /// </summary>
        Unsubscribe,

        /// <summary>
        /// Channel pattern subscribed
        /// </summary>
        PSubscribe,

        /// <summary>
        /// Message published to channel pattern
        /// </summary>
        PMessage,

        /// <summary>
        /// Channel pattern unsubsribed
        /// </summary>
        PUnsubscribe,
    }

    /// <summary>
    /// Redis existence specification for SET command
    /// </summary>
    public enum RedisExistence 
    { 
        /// <summary>
        /// Only set the key if it does not already exist
        /// </summary>
        Nx, 

        /// <summary>
        /// Only set the key if it already exists
        /// </summary>
        Xx,
    }

    public abstract class RedisRole
    {
        readonly string _roleName;
        public string RoleName { get { return _roleName; } }
        public RedisRole(string roleName)
        {
            _roleName = roleName;
        }
    }

    public class RedisMasterRole : RedisRole
    {
        readonly long _replicationOffset;
        readonly Tuple<string, int, int>[] _slaves;

        public long ReplicationOffset { get { return _replicationOffset; } }
        public Tuple<string, int, int>[] Slaves { get { return _slaves; } }

        internal RedisMasterRole(string role, long replicationOffset, Tuple<string, int, int>[] slaves)
            : base(role)
        {
            _replicationOffset = replicationOffset;
            _slaves = slaves;
        }
    }

    public class RedisSlaveRole : RedisRole
    {
        readonly string _masterIp;
        readonly int _masterPort;
        readonly string _replicationState;
        readonly long _dataReceived;

        public string MasterIp { get { return _masterIp; } }
        public int MasterPort { get { return _masterPort; } }
        public string ReplicationState { get { return _replicationState; } }
        public long DataReceived { get { return _dataReceived; } }

        internal RedisSlaveRole(string role, string masterIp, int masterPort, string replicationState, long dataReceived)
            : base(role)
        {
            _masterIp = masterIp;
            _masterPort = masterPort;
            _replicationState = replicationState;
            _dataReceived = dataReceived;
        }
    }

    public class RedisSentinelRole : RedisRole
    {
        readonly string[] _masters;

        public string[] Masters { get { return _masters; } }

        internal RedisSentinelRole(string role, string[] masters)
            : base(role)
        {
            _masters = masters;
        }
    }

    /// <summary>
    /// Represents the result of a Redis SCAN or SSCAN operation
    /// </summary>
    public class RedisScan<T>
    {
        /// <summary>
        /// Updated cursor that should be used as the cursor argument in the next call
        /// </summary>
        public long Cursor { get; set; }

        /// <summary>
        /// Collection of elements returned by the SCAN operation
        /// </summary>
        public T[] Items { get; set; }
    }

    public class RedisSubscriptionResponse
    {
        readonly string _channel;
        readonly string _pattern;
        readonly string _type;

        public string Channel { get { return _channel; } }
        public string Pattern { get { return _pattern; } }
        public string Type { get { return _type; } }

        public RedisSubscriptionResponse(string type, string channel, string pattern)
        {
            _type = type;
            _channel = channel;
            _pattern = pattern;
        }
    }

    public class RedisSubscriptionChannel : RedisSubscriptionResponse
    {
        readonly long _count;

        public long Count { get { return _count; } }

        public RedisSubscriptionChannel(string type, string channel, string pattern, long count)
            : base(type, channel, pattern)
        {
            _count = count;
        }
    }

    public class RedisSubscriptionMessage : RedisSubscriptionResponse
    {
        readonly string _body;

        public string Body { get { return _body; } }

        public RedisSubscriptionMessage(string type, string channel, string body)
            : base(type, channel, null)
        {
            _body = body;
        }

        public RedisSubscriptionMessage(string type, string pattern, string channel, string body)
            : base(type, channel, pattern)
        {
            _body = body;
        }
    }


    /// <summary>
    /// Base class for Redis server-info objects reported by Sentinel
    /// </summary>
    public abstract class RedisServerInfo : ISerializable
    {
        public RedisServerInfo(SerializationInfo info, StreamingContext context)
        {
            Name = info.GetString("name");
            Ip = info.GetString("ip");
            Port = info.GetInt32("port");
            RunId = info.GetString("runid");
            Flags = info.GetString("flags").Split(',');
            PendingCommands = info.GetInt64("pending-commands");
            LastOkPingReply = info.GetInt64("last-ok-ping-reply");
            LastPingReply = info.GetInt64("last-ping-reply");
            DownAfterMilliseconds = info.GetInt64("down-after-milliseconds");
        }

        /// <summary>
        /// Get or set Redis server name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Get or set Redis server IP
        /// </summary>
        public string Ip { get; set; }

        /// <summary>
        /// Get or set Redis server port
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Get or set Redis server run ID
        /// </summary>
        public string RunId { get; set; }

        /// <summary>
        /// Get or set Redis server flags
        /// </summary>
        public string[] Flags { get; set; }

        /// <summary>
        /// Get or set number of pending Redis server commands
        /// </summary>
        public long PendingCommands { get; set; }

        public long LastPingSent { get; set; }

        /// <summary>
        /// Get or set milliseconds since last successful ping reply
        /// </summary>
        public long LastOkPingReply { get; set; }

        /// <summary>
        /// Get or set milliseconds since last ping reply
        /// </summary>
        public long LastPingReply { get; set; }

        public long DownAfterMilliseconds { get; set; }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            throw new NotImplementedException();
        }
    }

    public abstract class RedisMasterSlaveInfo : RedisServerInfo
    {
        public RedisMasterSlaveInfo(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            InfoRefresh = info.GetInt64("info-refresh");
            RoleReported = info.GetString("role-reported");
            RoleReportedTime = info.GetInt64("role-reported-time");
        }

        public long InfoRefresh { get; set; }
        public string RoleReported { get; set; }
        public long RoleReportedTime { get; set; }
    }

    /// <summary>
    /// Represents a Redis master node as reported by a Redis Sentinel
    /// </summary>
    public class RedisMasterInfo : RedisMasterSlaveInfo
    {
        public RedisMasterInfo(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ConfigEpoch = info.GetInt64("config-epoch");
            NumSlaves = info.GetInt64("num-slaves");
            NumOtherSentinels = info.GetInt64("num-other-sentinels");
            Quorum = info.GetInt64("quorum");
            FailoverTimeout = info.GetInt64("failover-timeout");
            ParallelSyncs = info.GetInt64("parallel-syncs");
        }

        public long ConfigEpoch { get; set; }
        /// <summary>
        /// Get or set number of slaves of the current master node
        /// </summary>
        public long NumSlaves { get; set; }
        /// <summary>
        /// Get or set number of other Sentinels
        /// </summary>
        public long NumOtherSentinels { get; set; }
        /// <summary>
        /// Get or set Sentinel quorum count
        /// </summary>
        public long Quorum { get; set; }
        public long FailoverTimeout { get; set; }
        public long ParallelSyncs { get; set; }
    }



    /// <summary>
    /// Represents a Redis slave node as reported by a Redis Setinel
    /// </summary>
    public class RedisSlaveInfo : RedisMasterSlaveInfo
    {
        public RedisSlaveInfo(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MasterLinkDownTime = info.GetInt64("master-link-down-time");
            MasterLinkStatus = info.GetString("master-link-status");
            MasterHost = info.GetString("master-host");
            MasterPort = info.GetInt32("master-port");
            SlavePriority = info.GetInt64("slave-priority");
            SlaveReplOffset = info.GetInt64("slave-repl-offset");
        }

        public long MasterLinkDownTime { get; set; }

        /// <summary>
        /// Get or set status of master link
        /// </summary>
        public string MasterLinkStatus { get; set; }

        /// <summary>
        /// Get or set the master host of the current Redis slave node
        /// </summary>
        public string MasterHost { get; set; }

        /// <summary>
        /// Get or set the master port of the current Redis slave node
        /// </summary>
        public int MasterPort { get; set; }

        /// <summary>
        /// Get or set the priority of the current Redis slave node
        /// </summary>
        public long SlavePriority { get; set; }
        public long SlaveReplOffset { get; set; }
    }

    /// <summary>
    /// Represents a Redis Sentinel node as reported by a Redis Sentinel
    /// </summary>
    public class RedisSentinelInfo : RedisServerInfo
    {
        public RedisSentinelInfo(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            SDownTime = info.GetInt64("s-down-time");
            LastHelloMessage = info.GetInt64("last-hello-message");
            VotedLeader = info.GetString("voted-leader");
            VotedLeaderEpoch = info.GetInt64("voted-leader-epoch");
        }

        public long SDownTime { get; set; }

        /// <summary>
        /// Get or set milliseconds(?) since last hello message from current Sentinel node
        /// </summary>
        public long LastHelloMessage { get; set; }

        public string VotedLeader { get; set; }
        public long VotedLeaderEpoch { get; set; }
    }

    public class RedisSlowLogEntry
    {
        readonly long _id;
        readonly DateTime _date;
        readonly TimeSpan _latency;
        readonly string[] _arguments;

        public long Id { get { return _id; } }
        public DateTime Date { get { return _date; } }
        public TimeSpan Latency { get { return _latency; } }
        public string[] Arguments { get { return _arguments; } }

        public RedisSlowLogEntry(long id, DateTime date, TimeSpan latency, string[] arguments)
        {
            _id = id;
            _date = date;
            _latency = latency;
            _arguments = arguments;
        }
    }

    public class RedisMasterState
    {
        readonly long _downState;
        readonly string _leader;
        readonly long _voteEpoch;

        public long DownState { get { return _downState; } }
        public string Leader { get { return _leader; } }
        public long VoteEpoch { get { return _voteEpoch; } }

        public RedisMasterState(long downState, string leader, long voteEpoch)
        {
            _downState = downState;
            _leader = leader;
            _voteEpoch = voteEpoch;
        }
    }
}
