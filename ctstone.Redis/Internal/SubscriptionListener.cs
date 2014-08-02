using CSRedis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CSRedis.Internal
{
    class SubscriptionListener : RedisListner<RedisSubscriptionResponse>
    {
        long _count;

        public event Action<RedisSubscriptionMessage> MessageReceived;
        public event Action<RedisSubscriptionChannel> Changed;

        public SubscriptionListener(RedisConnection connection)
            : base(connection)
        { }

        public void Send(RedisSubscription command)
        {
            Write(command);
            if (!Listening)
                Listen(command.Parse);
        }

        protected override void OnParsed(RedisSubscriptionResponse response)
        {
            if (response is RedisSubscriptionChannel)
                OnReceivedChannel(response as RedisSubscriptionChannel);
            else if (response is RedisSubscriptionMessage)
                OnReceivedMessage(response as RedisSubscriptionMessage);
        }

        protected override bool Continue()
        {
            return _count > 0;
        }

        void OnReceivedChannel(RedisSubscriptionChannel channel)
        {
            _count = channel.Count;
            if (Changed != null)
                Changed(channel);
        }

        void OnReceivedMessage(RedisSubscriptionMessage message)
        {
            if (MessageReceived != null)
                MessageReceived(message);
        }
    }
}
