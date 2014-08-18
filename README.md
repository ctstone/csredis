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
    redis.ReconnectAttempts = 3;
    redis.ReconnectTimeout = 200;
    // connection will retry 3 times before throwing an Exception
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
Synchronous transactions are handled using the API calls MULTI/EXEC/DISCARD. Attach an event handler to **RedisClient.TransactionQueued** event to observe server queue replies (typically just 'OK'). When inside of a transaction, command return values will be default(T).
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


##Streaming responses (temporarily deprecated in v3)
For large result sizes, it may be preferred to stream the raw bytes from the server rather than allocating large chunks of memory in place. This can be achieved with **RedisClient.StreamTo()**. Note that this only applies to BULK responses (e.g. GET, HGET, LINDEX, etc). Attempting to stream any other response will result in an InvalidOperationException. Here is an example that stores the response in a MemoryStream 64 bytes at a time. A more useful example might use a FileStream and a larger buffer size.
```csharp
redis.Set("test", "lots-of-data-here");
using (var ms = new MemoryStream())
{
  redis.StreamTo(ms, 64, r => r.Get("test")); // small buffer size used for demo
  byte[] bytes = ms.ToArray(); // optional: get the bytes if needed
}
```

To access the raw bytes from a server response, use **RedisClient.BufferFor()** with **RedisClient.Read()**. Together, these two methods allow you to read any BULK server response a few bytes at a time. Note that the buffer *MUST* be emptied fully before issuing another command. The read buffer is considered empty when **Read()** returns 0 bytes read. Failing to empty the buffer before executing a new Redis command will result in an InvalidOperationException. Example:
```csharp
redis.Set("test", "lots-of-data-here");
redis.BufferFor(r => r.Get("test"));
byte[] buffer = new byte[64];
int bytes_read;
while ((bytes_read = redis.Read(buffer, 0, buffer.Length)) > 0)
{
  Console.WriteLine("Read {0} bytes : {1}", bytes_read, Encoding.UTF8.GetString(buffer, 0, bytes_read));
}
```

##Tracing
Placeholder: link to low-level .NET TCP tracing


##Sentinel
Rewired in v3. Examples coming soon
