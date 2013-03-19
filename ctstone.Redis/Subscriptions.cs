using System;

namespace ctstone.Redis
{
    /// <summary>
    /// Represents a Redis subscription message
    /// </summary>
    public class RedisSubscriptionMessage
    {
        /// <summary>
        /// Get or set the channel to which the message was published
        /// </summary>
        public string Channel { get; set; }

        /// <summary>
        /// Get or set the pattern that matched the published channel
        /// </summary>
        public string Pattern { get; set; }

        /// <summary>
        /// Get or set the message that was published
        /// </summary>
        public string Message { get; set; }
    }

    /// <summary>
    /// Represents a Redis subscribe/unsubscribe response
    /// </summary>
    public class RedisSubscriptionResponse
    {
        /// <summary>
        /// The type of response
        /// </summary>
        public RedisSubscriptionResponseType Type { get; set; }

        /// <summary>
        /// Count of channel subscriptions
        /// </summary>
        public long Count { get; set; }

        /// <summary>
        /// Message response
        /// </summary>
        public RedisSubscriptionMessage Message { get; set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionResponse class
        /// </summary>
        /// <param name="response">Server response</param>
        public RedisSubscriptionResponse(object[] response)
        {
            Type = (RedisSubscriptionResponseType)Enum.Parse(typeof(RedisSubscriptionResponseType), response[0] as String, true);
            switch (Type)
            {
                case RedisSubscriptionResponseType.Subscribe:
                case RedisSubscriptionResponseType.PSubscribe:
                    Message = new RedisSubscriptionMessage
                    {
                        Channel = response[1] as String,
                        Pattern = response[1] as String,
                    };
                    Count = (long)response[2];
                    break;

                case RedisSubscriptionResponseType.Message:
                    Message = new RedisSubscriptionMessage
                    {
                        Channel = response[1] as String,
                        Pattern = response[1] as String,
                        Message = response[2] as String,
                    };
                    break;

                case RedisSubscriptionResponseType.PMessage:
                    Message = new RedisSubscriptionMessage
                    {
                        Channel = response[1] as String,
                        Pattern = response[2] as String,
                        Message = response[3] as String,
                    };
                    break;

                case RedisSubscriptionResponseType.Unsubscribe:
                case RedisSubscriptionResponseType.PUnsubscribe:
                    Message = new RedisSubscriptionMessage
                    {
                        Channel = response[1] as String,
                        Pattern = response[1] as String,
                    };
                    Count = (long)response[2];
                    break;
            }
        }

        /// <summary>
        /// Get a string representation of the response
        /// </summary>
        /// <returns>Type and message of the response</returns>
        public override string ToString()
        {
            return String.Format("{0} - {1}", Type, Message);
        }
    }
}
