# csredis

csredis is a .NET client for Redis and Redis Sentinel (2.6). Includes both synchronous and asynchronous clients.

## Basic usage
Here are some simple commands using the **synchronous** client. Whenever possible, server responses are mapped to the appropriate CLR type.
```csharp
using (var redis = new RedisClient("localhost", 6379, 0))
{
  redis.Ping();
  string response = redis.Echo("hello world");
  DateTime server_time = redis.Time();
}
```

The **asynchronous** client uses the .NET task framework and requires .NET4. Here is an example showing four ways to handle the async Task
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  // fire-and-forget
  for (int i = 0; i < 5000; i++)
  {
    redis.Incr("test1");
  }
  
  // callback via ContinueWith
  redis.Ping().ContinueWith(t => Console.WriteLine(t.Result));
  
  // blocking
  var t = redis.Get("test1");
  t.Wait();
  var result = t.Result;
  
  // blocking helper
  var result = redis.Wait(r => r.Get("test1"));
}
```

##Authentication
Password authentication is handled just like the native API:
```csharp
redis.Auth("mystrongpasword");
```

##Flexible hash mapping
Pass any POCO or anonymous object to the generic hash methods:
redis.HMSet("myhash", new
{
  Field1 = "string",
  Field2 = true,
  Field3 = DateTime.Now,
});

Or use a string Dictionary:
redis.HMSet("mydict", new Dictionary<string, string>
{
  { "F1", "string" },
  { "F2", "true" },
  { "F3", DateTime.Now.ToString() },
});

Or use the native API:
redis.HMSet("myhash", new[] { "F1", "string", "F2", "true", "F3", DateTime.Now.ToString() });

##Subscription model
Because subscriptions block the active connection, subscriptions are not supported in RedisClientAsync.

The subscription model is event based. Attach a handler to one or both of SubscriptionChanged/SubscriptionReceived to receive callbacks on subscription events. Pattern/non-pattern subscriptions are handled by the same events. Unsubscription will have to be handled by a background thread/task.

SubscriptionChanged: Occurs when a subsciption channel is opened or closed
RedisSubscriptionReceived: Occurs when a subscription message has been received

Example:
```csharp
redis.SubscriptionChanged += (s, ev) =>
{
  Console.WriteLine("There are now {0} open channels", ev.Response.Count);
};
redis.SubscriptionReceived += (s, ev) =>
{
  Console.WriteLine("Message received: {0}", ev.Message.Body);
};
redis.PSubscribe("*");
```

##Sentinel
Sentinel is the monitoring/high-availability server packaged with the Redis server. Sentinel is not yet widely documented, but csredis supports the specification as closely as possible.

RedisSentinelManager can be thought of as a dispatcher between you and the current master Redis server (or other Sentinels, or a Redis slave). Pass one or more Redis Sentinel hosts to the Manager and then query for an active master or slave:
```csharp
RedisSentinelManager sentman = new RedisSentinelManager("localhost:26379");
RedisClient master = sentman.GetMaster("mymaster", 100, 100);
RedisClient slave = sentman.GetSlave("mymaster", 100, 100);
```

Note that RedisSentinelManager currently does not yet support RedisClientAsync.

If you need direct access to a Sentinel, RedisSentinelManager keeps track of the last Sentinel that responded:
```csharp
RedisSentinelManager sentman = new RedisSentinelManager("localhost:26379");
using (var sentinel = sentman.GetSentinel(100))
{
  Tuple<string, int> master = sentinel.GetMasterAddrByName("mymaster");
}
```

RedisSentinelClient supports the same subscription model as RedisClient (see above).
