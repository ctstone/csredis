using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ctstone.Redis.Handlers
{
    class RedisSubscriptionHandler
    {
        private readonly RedisConnection _connection;

        public bool IsSubscribed { get; private set; }
        public long Count { get; private set; }

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        public RedisSubscriptionHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public void HandleSubscription(RedisSubscription command)
        {
            _connection.Write(command.Command, command.Arguments);
            if (!IsSubscribed)
            {
                using (new ActivityTracer("Handle subscriptions"))
                {
                    IsSubscribed = true;
                    while (true)
                    {
                        var resp = _connection.Read(command.Parser);
                        switch (resp.Type)
                        {
                            case RedisSubscriptionResponseType.Subscribe:
                            case RedisSubscriptionResponseType.PSubscribe:
                            case RedisSubscriptionResponseType.Unsubscribe:
                            case RedisSubscriptionResponseType.PUnsubscribe:
                                RedisSubscriptionChannel channel = resp as RedisSubscriptionChannel;
                                Count = channel.Count;
                                if (SubscriptionChanged != null)
                                    SubscriptionChanged(this, new RedisSubscriptionChangedEventArgs(channel));
                                break;

                            case RedisSubscriptionResponseType.Message:
                            case RedisSubscriptionResponseType.PMessage:
                                RedisSubscriptionMessage message = resp as RedisSubscriptionMessage;
                                if (SubscriptionReceived != null)
                                    SubscriptionReceived(this, new RedisSubscriptionReceivedEventArgs(message));
                                break;
                        }
                        if (Count == 0)
                            break;
                    }
                    IsSubscribed = false;
                }
            }
        }
    }
}
