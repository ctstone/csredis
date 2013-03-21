# csredis

csredis is a .NET client for Redis and Redis Sentinel (2.6). Includes both synchronous and asynchronous implementations.

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

The **asynchronous** client uses the .NET task framework and requires .NET4. Here is an example showing four ways to work with the async Task
```csharp
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  // fire-and-forget
  for (int i = 0; i < 5000; i++)
  {
    redis.Incr("test1");
  }
  
  // callback via ContinueWith
  redis.Ping().ContinueWith(t => Console.WriteLine(t.Result));
  
  // blocking helper
  string result = redis.Wait(r => r.Get("test1"));
  
  // blocking verbose
  var t = redis.Get("test1");
  t.Wait();
  string result = t.Result;
}
```

##Pipeline
RedisClient supports pipelining commands to lessen the effects of network overhead (RedisClientAsync achieves this automatically due to its asynchronous nature). To enable pipelining, just wrap a group of commands between StartPipe() and EndPipe(). Note that redis-server currently has a 1GB limit on client buffers, so don't go over that :)
```csharp
redis.BeginPipe();
redis.Echo("hello"); // returns immediately with default(string)
redis.Time(); // returns immediately with default(DateTime)
object[] result = redis.EndPipe(); // get the server response for processing
redis.EndPipe(true); // ignore results (fire-and-forget). This also keeps memory overhead low for large batches.
```

##Why csredis?
There are a handful of .NET redis clients out in the wild, but none perfectly suited my needs: clean interface of the native Redis API; Sentinel support; easy-to-use pipelining/async. csredis is probably missing a few niche features supported by other projects. For intance, if you need atomic database selection with every command, csredis may not be for you.

##Benchmarks
Test case: 5000 pipelined/async INCR iterations on the same key, then waiting for a single GET. All times averaged over 5 attempts to the same Redis server on the Internet. Measured using Diagnostics.Stopwatch.
* **csredis (async)** 274.4ms
* **csredis** 277.4ms
* **booksleeve** 315.2ms
* **servicestack** 497.4ms

##Authentication
Password authentication is handled according to the native API (i.e. not in the connection constructor):
```csharp
redis.Auth("mystrongpasword");
```

##Flexible hash mapping
Pass any POCO or anonymous object to the generic hash methods:
```chsarp
redis.HMSet("myhash", new
{
  Field1 = "string",
  Field2 = true,
  Field3 = DateTime.Now,
});

MyPOCO hash = redis.HGetAll<MyPOCO>("my-hash-key");
```

Or use a string Dictionary:
```chsarp
redis.HMSet("mydict", new Dictionary<string, string>
{
  { "F1", "string" },
  { "F2", "true" },
  { "F3", DateTime.Now.ToString() },
});

Dictionary<string, string> mydict = redis.HGetAll("my-hash-key");
```

Or use the native API:
```csharp
redis.HMSet("myhash", new[] { "F1", "string", "F2", "true", "F3", DateTime.Now.ToString() });
```

##Async exception handling
csredis exceptions can be split into two groups: fatal and non-fatal. Non-fatal exceptions are raised when the Redis server responds with an error message (e.g. when trying to increment a string by 1) or if there is a protocol violoation (e.g. we expected a bulk reply but ended up with something else). These exceptions are **RedisException** and **RedisProtocolException**, respectively. The assumption is that these errors are isloated to a single Redis command request and should not necessarily affect subsequent requests.

All other exceptions are considered fatal: something in the network stack or a bug in the implementation. These exceptions will propogate back to main thread when the RedisClientAsync is disposed.

All task exceptions are passed to the **RedisClientAsync.ExceptionOccurred** event. The user may attach to this event to observe fatal and non-fatal exceptions as they are thrown, rather than waiting for Dispose() to bring the AggregateException into scope.
```csharp
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  redis.ExceptionOccurred += (s, a) =>
  {
    Console.WriteLine(a.ExceptionObject);
  };
  redis.Set("not-a-number", "test");
  redis.Incr("not-a-number");
}
```

Of course, the canonical method for handling exceptions in the TPL is to Wait() for a task in the main thread and and then wrap it with a try/catch AggregateException. This however, requires a lot more attention for each task than the event handler documented above:
```csharp
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  redis.Set("not-a-number", "test");
  var t = redis.Incr("not-a-number");
  try
  {
    t.Wait();
  }
  catch (AggregateException ae)
  {
    foreach (var inner in ae.InnerExceptions)
      Console.log(inner);
  }
}
```

##Subscription model
Because subscriptions block the active connection, subscriptions are not supported in RedisClientAsync.

The subscription model is event based. Attach a handler to one or both of SubscriptionChanged/SubscriptionReceived to receive callbacks on subscription events. Pattern and non-pattern channels are handled by the same events. Opening a subscription channel blocks the main thread, so unsubscription will need to be handled by a background thread/task.

**SubscriptionChanged**: Occurs when a subsciption channel is opened or closed  
**RedisSubscriptionReceived**: Occurs when a subscription message has been received

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

##Future-proof
All clients support the generic Call() method to send arbitrary commands to the Redis or Sentinel servers. Use this command to easily implement future Redis commands before they are included in csredis. Or use this command to work with bare-metal responses or if you renamed a command in your redis.conf.
```csharp
object resp = redis.Call("ANYTHING", "arg1", "arg2", "arg3");
```
Note that the response object will need to be cast according to the Redis unified protocol: status (System.String), integer (System.Int32), bulk (System.String), multi-bulk (System.Object[])

##Sentinel
Sentinel is the monitoring/high-availability server packaged with Redis server. Sentinel is not yet widely documented, but csredis supports the specification as closely as possible.

RedisSentinelManager can be thought of as a dispatcher between you and the current master Redis server (or another Sentinel, or a Redis slave). Pass one or more Redis Sentinel hosts to the Manager and then query for an active master or slave:
```csharp
RedisSentinelManager sentman = new RedisSentinelManager("localhost:26379");
RedisClient master = sentman.GetMaster("mymaster", 100, 100);
RedisClient slave = sentman.GetSlave("mymaster", 100, 100);
```
RedisSentinelManager currently does not yet support RedisClientAsync.

If you need direct access to a Sentinel, RedisSentinelManager keeps track of the last Sentinel that responded:
```csharp
RedisSentinelManager sentman = new RedisSentinelManager("localhost:26379");
using (var sentinel = sentman.GetSentinel(100))
{
  Tuple<string, int> master = sentinel.GetMasterAddrByName("mymaster");
}
```

RedisSentinelClient supports the same subscription event model as RedisClient (see above). Consult http://redis.io/ for a list of channels that Sentinel publishes. The client may not publish its own messages.