using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ctstone.Redis.Internal
{
    class MonitorListener : RedisListner<object>
    {
        public event Action<object> MonitorReceived;

        public MonitorListener(RedisConnection connection)
            : base(connection)
        { }

        public string Start()
        {
            string status = Call(RedisCommand.Monitor());
            Listen(x => x.Read());
            return status;
        }

        protected override void OnParsed(object value)
        {
            OnMonitorReceived(value);
        }

        protected override bool Continue()
        {
            return Connection.Connected;
        }

        void OnMonitorReceived(object message)
        {
            if (MonitorReceived != null)
                MonitorReceived(message);
        }
    }
}
