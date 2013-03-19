using System;

namespace ctstone.Redis
{
    /// <summary>
    /// Represents a Redis server error reply
    /// </summary>
    public class RedisException : Exception
    {
        /// <summary>
        /// Instantiate a new instance of the RedisException class
        /// </summary>
        /// <param name="message">Server response</param>
        public RedisException(string message)
            : base(message)
        { }
    }

    /// <summary>
    /// The exception that is thrown when an unexpected value is found in a Redis request or response 
    /// </summary>
    public class RedisProtocolException : Exception
    {
        /// <summary>
        /// Instantiate a new instance of the RedisProtocolException class
        /// </summary>
        /// <param name="message">Protocol violoation message</param>
        public RedisProtocolException(string message)
            : base(message)
        { }
    }
}
