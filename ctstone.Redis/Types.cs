
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
    /// Redis sub-command for SLOWLOG command
    /// </summary>
    public enum RedisSlowLog
    {
        /// <summary>
        /// Return entries in the slow log
        /// </summary>
        Get,

        /// <summary>
        /// Get the length of the slow log
        /// </summary>
        Len,

        /// <summary>
        /// Delete all information from the slow log
        /// </summary>
        Reset,
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

}
