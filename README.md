# csredis [![Build status](https://ci.appveyor.com/api/projects/status/cfhtnvf9vuyf5797)](https://ci.appveyor.com/project/ctstone/csredis-675)

CSRedis is a .NET client for Redis and Redis Sentinel (2.8.12). Includes both synchronous and asynchronous implementations.

The easiest way to install CSRedis is from [NuGet](https://nuget.org/packages/csredis) via the [Package Manager Console](http://docs.nuget.org/docs/start-here/using-the-package-manager-console):  

**PM> Install-Package csredis**


## Basic usage
Whenever possible, server responses are mapped to the appropriate CLR type.
```csharp
using (var redis = new RedisClient("yourhost"))
{
    string ping = redis.Ping();
    string echo = redis.Echo("hello world");
    DateTime time = redis.Time();
}
```

**Asynchronous** commands are also available.
```csharp
using (var redis = new RedisClient("localhost"))
{
    // fire-and-forget: results are not captured
    for (int i = 0; i < 5000; i++)
    {
        redis.IncrAsync("test1");
    }

    // callback via ContinueWith: Ping is executed asyncronously, and when a result is ready, the response is printed to screen.
    redis.TimeAsync().ContinueWith(t => Console.WriteLine(t.Result));

    // blocking call
    string result = redis.GetAsync("test1").Result;
}
```

Use the IRedisClient or IRedisClientAsync interfaces to use synconous or asyncronous methods exclusively.
```csharp
using (IRedisClient csredis = new RedisClient(Host))
{
    // only syncronous methods exposed
}

using (IRedisClientAsync csredis = new RedisClient(Host))
{
    // only asyncronous methods exposed
}
```

##Pipelining
CSRedis supports pipelining commands to lessen the effects of network overhead on sequential server calls. To enable pipelining, wrap a group of commands between **StartPipe()** and **EndPipe()**. Note that redis-server currently has a 1GB limit on client buffers but CSRedis does not enforce this. Similar performance gains may be obtained by using the deferred Task/Asyncronous methods.
```csharp
using (var redis = new RedisClient("localhost"))
{
    redis.StartPipe();
    var empty1 = redis.Echo("hello"); // returns immediately with default(string)
    var empty2 = redis.Time(); // returns immediately with default(DateTime)
    object[] result = redis.EndPipe(); // all commands sent to the server at once
    var item1 = (string)result[0]; // cast result objects to appropriate types
    var item2 = (DateTime)result[1];

    // automatic MULTI/EXEC pipeline: start a pipe that is also a MULTI/EXEC transaction
    redis.StartPipeTransaction();
    redis.Set("key", "value");
    redis.Set("key2", "value2");
    object[] result2 = redis.EndPipe(); // transaction is EXEC'd automatically if DISCARD was not called first
    
    // DISCARD pipelined transaction
    redis.StartPipeTransaction();
    redis.Set("key", 123);
    redis.Set("key2", "abc");
    redis.Discard();
}
```


##Why csredis?
There are a handful of .NET redis clients in active development, but none quite suited my needs: clean interface of the native Redis API; Sentinel support; easy-to-use pipelining/async. If there are gaps between CSRedis and another implementation please open an Issue or Pull Request.


##Authentication
Password authentication is handled according to the native API (i.e. not in the connection constructor):
```csharp
redis.Auth("mystrongpasword");
```

##Reconnecting
CSRedis supports a simple reconnect option to handle dropped connections to the same Redis host. See **RedisSentinelManager** for a fuller implementation between multiple masters.
```csharp
using (var redis = new RedisClient("localhost"))
{
    redis.Connected += (s, e) => redis.Auth(Password); // set AUTH, CLIENT NAME, etc
    redis.ReconnectAttempts = 3;
    redis.ReconnectWait = 200;
    // connection will retry 3 times with 200ms in between before throwing an Exception
}
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


##Transactions
Synchronous transactions are handled using the API calls MULTI/EXEC/DISCARD. Attach an event handler to **RedisClient.TransactionQueued** event to observe server queue replies (typically 'OK'). When inside of a transaction, command return values will be default(T).
```csharp
redis.TransactionQueued += (s, e) =>
{
    Console.WriteLine("Transaction queued: {0}({1}) = {2}", e.Command, String.Join(", ", e.Arguments), e.Status);
};
redis.Multi();
var empty1 = redis.Set("test1", "hello"); // returns default(String)
var empty2 = redis.Set("test2", "world"); // returns default(String)
var empty3 = redis.Time(); // returns default(DateTime)
object[] result = redis.Exec();
var item1 = (string)result[0];
var item2 = (string)result[1];
var item3 = (DateTime)result[2];
```


##Subscription model
The subscription model is event based. Attach a handler to one or both of SubscriptionChanged/SubscriptionReceived to receive callbacks on subscription events. Opening the first subscription channel blocks the main thread, so unsubscription (and new subscriptions) must be handled by a background thread/task.

**SubscriptionChanged**: Occurs when a subsciption channel is opened or closed  
**RedisSubscriptionReceived**: Occurs when a subscription message has been received

Example:
```csharp
redis.SubscriptionChanged += (s, e) =>
{
  Console.WriteLine("There are now {0} open channels", e.Response.Count);
};
redis.SubscriptionReceived += (s, e) =>
{
  Console.WriteLine("Message received: {0}", e.Message.Body);
};
redis.PSubscribe("*");
```

##Future-proof
CSRedis exposes a basic **Call()** method that sends arbitrary commands to the Redis server. Use this command to easily implement future Redis commands before they are included in CSRedis. This can also be used to work with "bare-metal" server responses or if a command has been renamed in redis.conf.
```csharp
object resp = redis.Call("ANYTHING", "arg1", "arg2", "arg3");
```
Note that the response object will need to be cast according to the Redis unified protocol: status (System.String), integer (System.Int64), bulk (System.String), multi-bulk (System.Object[]).


##Streaming responses
For large result sizes, it may be preferred to stream the raw bytes from the server rather than allocating large chunks of memory in place. This can be achieved with **RedisClient.StreamTo()**. Note that this only applies to BULK responses (e.g. GET, HGET, LINDEX, etc). Attempting to stream any other response will result in an InvalidOperationException. Here is an example that stores the response in a MemoryStream 64 bytes at a time. A more useful example might use a FileStream and a larger buffer size.
```csharp
redis.Set("test", new string('x', 1048576)); // 1MB string
using (var ms = new MemoryStream())
{
    redis.StreamTo(ms, 64, r => r.Get("test")); // read in small 64 byte blocks
    byte[] bytes = ms.ToArray(); // optional: get the bytes if needed
}
```

##Tracing
Use [.NET tracing](http://msdn.microsoft.com/en-us/library/ms733025(v=vs.110).aspx) to expose low level TCP messages


##Sentinel
**RedisSentinelManager** is a managed connection that will automatically obtain a connection to a Redis master node based on information from one or more Redis Sentinel nodes. Async methods coming soon
```csharp
using (var sentinel = new RedisSentinelManager("host1:123", "host2:456"))
{
    sentinel.Add(Host); // add host using default port 
    sentinel.Add(Host, 36379); // add host using specific port
    sentinel.Connected += (s, e) => sentinel.Call(x => x.Auth(Password)); // this will be called each time a master connects
    sentinel.Connect("mymaster"); // open connection
    var test2 = sentinel.Call(x => x.Time()); // use the Call() lambda to access the current master connection
}
```
