using System;

namespace ctstone.Redis
{
    /// <summary>
    /// Base class for Redis pub/sub responses
    /// </summary>
    public abstract class RedisSubscriptionResponse
    {
        /// <summary>
        /// The type of response
        /// </summary>
        public RedisSubscriptionResponseType Type { get; private set; }

        /// <summary>
        /// Get the channel to which the message was published, or null if not available
        /// </summary>
        public string Channel { get; protected set; }

        /// <summary>
        /// Get the pattern that matched the published channel, or null if not available
        /// </summary>
        public string Pattern { get; protected set; }

        /// <summary>
        /// Read multi-bulk response from Redis server
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        public static RedisSubscriptionResponse ReadResponse(object[] response)
        {
            RedisSubscriptionResponseType type  = ParseType(response[0] as String);

            RedisSubscriptionResponse obj;
            switch (type)
            {
                case RedisSubscriptionResponseType.Subscribe:
                case RedisSubscriptionResponseType.Unsubscribe:
                case RedisSubscriptionResponseType.PSubscribe:
                case RedisSubscriptionResponseType.PUnsubscribe:
                    obj = new RedisSubscriptionChannel(type, response);
                    break;

                case RedisSubscriptionResponseType.Message:
                case RedisSubscriptionResponseType.PMessage:
                    obj = new RedisSubscriptionMessage(type, response);
                    break;

                default:
                    throw new RedisProtocolException("Unexpected response type: " + type);
            }
            obj.Type = type;
            return obj;
        }

        private static RedisSubscriptionResponseType ParseType(string type)
        {
            return (RedisSubscriptionResponseType)Enum.Parse(typeof(RedisSubscriptionResponseType), type, true);
        }
    }

    /// <summary>
    /// Represents a Redis channel in a pub/sub context
    /// </summary>
    public class RedisSubscriptionChannel : RedisSubscriptionResponse
    {
        /// <summary>
        /// Get the number of subscription channels currently open on the current connection
        /// </summary>
        public long Count { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionChannel class
        /// </summary>
        /// <param name="type">The type of channel response</param>
        /// <param name="response">Redis multi-bulk response</param>
        public RedisSubscriptionChannel(RedisSubscriptionResponseType type, object[] response)
        {
            switch (type)
            {
                case RedisSubscriptionResponseType.Subscribe:
                case RedisSubscriptionResponseType.Unsubscribe:
                    Channel = response[1] as String;
                    break;

                case RedisSubscriptionResponseType.PSubscribe:
                case RedisSubscriptionResponseType.PUnsubscribe:
                    Pattern = response[1] as String;
                    break;
            }
            Count = (long)response[2];
        }
    }

    /// <summary>
    /// Represents a Redis message in a pub/sub context
    /// </summary>
    public class RedisSubscriptionMessage : RedisSubscriptionResponse
    {
        /// <summary>
        /// Get the message that was published
        /// </summary>
        public string Body { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionMessage class
        /// </summary>
        /// <param name="type">The type of message response</param>
        /// <param name="response">Redis multi-bulk response</param>
        public RedisSubscriptionMessage(RedisSubscriptionResponseType type, object[] response)
        {
            switch (type)
            {
                case RedisSubscriptionResponseType.Message:
                    Channel = response[1] as String;
                    Body = response[2] as String;
                    break;

                case RedisSubscriptionResponseType.PMessage:
                    Pattern = response[1] as String;
                    Channel = response[2] as String;
                    Body = response[3] as String;
                    break;
            }
        }
    }
}
