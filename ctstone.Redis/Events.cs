using System;

namespace ctstone.Redis
{
    /// <summary>
    /// Provides data for the event that is raised when a subscription channel is opened or closed
    /// </summary>
    public class RedisSubscriptionChangedEventArgs : EventArgs
    {
        /// <summary>
        /// The subscription response
        /// </summary>
        public RedisSubscriptionChannel Response { get; private set; }

        /// <summary>
        /// Instantiate new instance of the RedisSubscriptionChangedEventArgs class
        /// </summary>
        /// <param name="response">The Redis server response</param>
        public RedisSubscriptionChangedEventArgs(RedisSubscriptionChannel response)
        {
            Response = response;
        }
    }

    /// <summary>
    /// Provides data for the event that is raised when a subscription message is received
    /// </summary>
    public class RedisSubscriptionReceivedEventArgs : EventArgs
    {
        /// <summary>
        /// The subscription message
        /// </summary>
        public RedisSubscriptionMessage Message { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisSubscriptionReceivedEventArgs class
        /// </summary>
        /// <param name="message">The Redis server message</param>
        public RedisSubscriptionReceivedEventArgs(RedisSubscriptionMessage message)
        {
            Message = message;
        }
    }

    /// <summary>
    /// Provides data for the event that is raised when a transaction command has been processed by the server
    /// </summary>
    public class RedisTransactionQueuedEventArgs : EventArgs
    {
        /// <summary>
        /// The status code of the transaction command
        /// </summary>
        public string Status { get; private set; }

        /// <summary>
        /// The command that was queued
        /// </summary>
        public string Command { get; private set; }

        /// <summary>
        /// The arguments of the queued command
        /// </summary>
        public object[] Arguments { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisTransactionQueuedEventArgs class
        /// </summary>
        /// <param name="command">Command text</param>
        /// <param name="arguments">Command arguments</param>
        /// <param name="status">Server status code</param>
        public RedisTransactionQueuedEventArgs(string status, string command, object[] arguments)
        {
            Status = status;
            Command = command;
            Arguments = arguments;
        }
    }

    /// <summary>
    /// Provides data for the event that is raised when a MULTI/EXEC transaction is initiated with the server
    /// </summary>
    public class RedisTransactionStartedEventArgs : EventArgs
    {
        /// <summary>
        /// The status code of the transaction command
        /// </summary>
        public string Status { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisTransactionStartedEventArgs class
        /// </summary>
        /// <param name="status">Server status code</param>
        public RedisTransactionStartedEventArgs(string status)
        {
            Status = status;
        }
    }

    /// <summary>
    /// Provides data for the event that is raised when a Redis MONITOR message is received
    /// </summary>
    public class RedisMonitorEventArgs : EventArgs
    {
        /// <summary>
        /// Monitor output
        /// </summary>
        public object Message { get; private set; }

        /// <summary>
        /// Instantiate a new instance of the RedisMonitorEventArgs class
        /// </summary>
        /// <param name="message">The Redis server message</param>
        public RedisMonitorEventArgs(object message)
        {
            Message = message;
        }
    }
}
