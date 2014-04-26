using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ctstone.Redis.IO;

namespace ctstone.Redis.Handlers
{
    class RedisMonitorHandler
    {
        private readonly RedisConnection _connection;

        /// <summary>
        /// Occurs when a monitor response is received
        /// </summary>
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        public RedisMonitorHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public string Monitor()
        {
            using (new ActivityTracer("Beging monitor"))
            {
                string status = _connection.Call(RedisReader.ReadStatus, "MONITOR");
                while (true)
                {
                    object message;
                    try
                    {
                        message = _connection.Read();
                    }
                    catch (Exception e)
                    {
                        if (_connection.Connected) throw e;
                        return status;
                    }
                    if (MonitorReceived != null)
                        MonitorReceived(this, new RedisMonitorEventArgs(message));
                }
            }
        }
    }
}
