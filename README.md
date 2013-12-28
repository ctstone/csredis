# csredis

csredis is a .NET client for Redis and Redis Sentinel (2.8). Includes both synchronous and asynchronous implementations.

The easiest way to install csredis is from [NuGet](https://nuget.org/packages/csredis) via the [Package Manager Console](http://docs.nuget.org/docs/start-here/using-the-package-manager-console):  

**PM> Install-Package csredis**


## Basic usage
Here are some simple commands using the **synchronous** client. Whenever possible, server responses are mapped to the appropriate CLR type.
```csharp
using (var redis = new RedisClient("yourhost"))
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
  // fire-and-forget: results are not captured
  for (int i = 0; i < 5000; i++)
  {
    redis.Incr("test1");
  }
  
  // callback via ContinueWith: Ping is executed asyncronously, and when a result is ready, the response is printed to screen.
  redis.Ping().ContinueWith(t => Console.WriteLine(t.Result));
  
  // blocking helper: built in helper to execute requests syncronously
  string result = redis.Wait(r => r.Get("test1"));
  
  // blocking verbose: same as above, with more control over wait timeout
  var t = redis.Get("test1");
  t.Wait(TimeSpan.FromSeconds(3));
  string result = t.Result;
}
```

Blocking or otherwise *non-thread-safe commands are not directly available from RedisClientAsync*. See below for notes on opening a dedicated connection from RedisClientAsync for use with subscriptions, transactions, and blocking list pops.


##Pipelining
RedisClient supports pipelining commands to lessen the effects of network overhead (RedisClientAsync achieves this automatically due to its asynchronous nature). To enable pipelining, just wrap a group of commands between **StartPipe()** and **EndPipe()**. Note that redis-server currently has a 1GB limit on client buffers.
```csharp
redis.StartPipe();
redis.Echo("hello"); // returns immediately with default(string)
redis.Time(); // returns immediately with default(DateTime)
object[] result = redis.EndPipe(); // get the server response for processing
string item1 = result[0] as String; // cast result objects to appropriate types
DateTime item2 = (DateTime)result[1]; 

// automatic MULTI/EXEC pipeline: start a pipe that is also a MULTI/EXEC transaction
redis.StartPipeTransaction();
redis.Set("key", "value");
redis.Set("key2", "value2");
object[] result2 = redis.EndPipe(); // transaction is EXEC'd automatically if DISCARD was not called first

// ignoring result parsing: use for lower memory footprint when server responses do not need to be checked.
redis.StartPipe(false);
// ...
redis.EndPipe();
```


##Why csredis?
There are a handful of .NET redis clients out in the wild, but none perfectly suited my needs: clean interface of the native Redis API; Sentinel support; easy-to-use pipelining/async. csredis is probably missing a few niche features supported by other projects. For intance, if you need atomic database selection with every command, csredis may not be for you.


##Is csredis stable?
Not yet. RedisClient (synchronous) has a near-full test suite, all of which must pass before committing/push. RedisClientAsync currently has no tests (coming soon!) and as such should be considered unstable.


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

All task exceptions are passed to the **RedisClientAsync.ExceptionOccurred** event. The user may attach to this event to observe fatal and non-fatal exceptions as they are thrown, rather than waiting for **Dispose()** to bring the AggregateException into scope.
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

Of course, the canonical method for handling exceptions in the TPL is to **Wait()** for a task in the main thread and and then wrap it with a try/catch AggregateException. This however, requires a lot more attention for each task than the event handler documented above:
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


##Transactions
Synchronous transactions are handled using the API calls MULTI/EXEC/DISCARD. Asynchronous transactions should be handled using the Clone() method (see below). Server response to MULTI is not returned directly to the caller. Attach to **RedisClient.TransactionStarted** event to observe this reply. Similarly, when inside of a transaction, command return values will be default(T). Actual server status response (i.e. "QUEUED") may be observed by attaching to the **RedisClient.TransactionQueued** event.
```csharp
redis.TransactionStarted += (s, e) =>
{
    Console.WriteLine("Transaction started: {0}", e.Status);
};
redis.TransactionQueued += (s, e) =>
{
    Console.WriteLine("Transaction queued: {0}({1}) = {2}", e.Command, String.Join(", ", e.Arguments), e.Status);
};
redis.Multi();
redis.Set("test1", "hello"); // returns default(String)
redis.Set("test2", "world"); // returns default(String)
redis.Time(); // returns default(DateTime)
object[] result = redis.Exec();
string item1 = result[0] as String; // cast result items to parsed tyep
string item2 = result[1] as String;
string item3 = (DateTime)result[2];
```

**Asynchronous transactions** affect all commands on the current connection, so a new connection must be opened to ensure thread-safety. Use **Clone()** to open a single-threaded connection to the current redis server. **Clone** may also be used to execute blocking commands against the current Redis server without blocking other async operations.
```csharp
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  using (var tr = redis.Clone()) // use tr only on a single thread
  {
    tr.StartPipeTransaction(); // optional: starting transaction in pipeline mode
    tr.Set("test1", "hello");
    tr.Set("test2", "world");
    object[] result = tr.EndPipe(); // transaction is EXEC'd by EndPipe()
  }
  
  // continue to use redis object asyncronously on multiple threads
  redis.Set("hello", "world");
  
  // use Clone() to execute a blocking command on the current Redis server
  using (var blocking = redis.Clone())
  {
    var value = blocking.BLPop(1000, "my-list"); // block cloned connection for up to 1 second
  }
}
```


##Subscription model
The subscription model is event based. Attach a handler to one or both of SubscriptionChanged/SubscriptionReceived to receive callbacks on subscription events. When using the syncronous RedisClient, opening the first subscription channel blocks the main thread, so unsubscription (and new subscriptions) must be handled by a background thread/task. See below for thread-safe usage.

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

**Non-blocking, thread-safe subscription client**  
Use the non-blocking subscription client if you prefer not to block your RedisClient instance.
```csharp
using (var sub = new RedisSubscriptionClient("localhost", "6379", "my-password"))
{
  sub.SubscriptionReceived += (s, e) => Console.WriteLine(e.Message.Body); // global message handler
  sub.Subscribe("channel 1");
  sub.Subscribe(x => Console.WriteLine(x.Body), "channel 2"); // with callback just for "channel 2"
  
  // keep thread alive
  Thread.Sleep(10000);
}
```

To use **subscriptions with RedisClientAsync**, a new dedicated connection must be opened to the Redis server. To access the shared, thread-safe subscription channel, use the SubscriptionClient property:
```csharp
using (var redis = new RedisClientAsync("localhost", 6379, 0))
{
  redis.SubscriptionChannel.SubscriptionChanged += (s, a) => Console.WriteLine(a.Message.Body); // global message handler
  redis.SubscriptionChannel.Subscribe(x => Console.WriteLine(x.Body), "channel 1"); // with callback just for "channel 1"
  redis.SubscriptionChannel.Subscribe("channel 1"); // no channel-specific callback
  
  // keep thread alive
  Thread.Sleep(10000);
}
```


##Future-proof
All csredis clients support a basic **Call()** method that sends arbitrary commands to the Redis server. Use this command to easily implement future Redis commands before they are included in csredis. This can also be used to work with "bare-metal" server responses or if a command has been renamed in redis.conf.
```csharp
object resp = redis.Call("ANYTHING", "arg1", "arg2", "arg3");
```
Note that the response object will need to be cast according to the Redis unified protocol: status (System.String), integer (System.Int32), bulk (System.String), multi-bulk (System.Object[]).


##Streaming responses
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
csredis supports the .NET tracing model in order to debug production instances. To enable csredis traces in your application, add the following section to your app.config or web.config:
```xml
<system.diagnostics>
  <trace autoflush="true" />
  <sources>
    <source name="csredis" switchValue="Verbose">
      <listeners>
        <add
          name="csredis.XmlWriterTraceListener"
          type="System.Diagnostics.XmlWriterTraceListener"
          initializeData="csredis.svclog"
          traceOutputOptions="LogicalOperationStack,Callstack"/>
      </listeners>
    </source>
  </sources>
</system.diagnostics>
```
In this example, I am writing at a verbose level to an XML listener. Output includes the operation stack and the callstack for each trace. Read more about switchLevel, listeners, and traceOutputOptions at [MSDN](http://msdn.microsoft.com/en-us/library/zs6s4h68.aspx).  Other trace listeners exist for logging to console, flat file, or event log.

The csredis tracing implementation is experimental and is subject to change. Do not enable without first taking note of the significant performance cost.

csredis tracing output includes information on each opened/closed connection; exact comand output sent to server (readable text plus unified protocol); and response types received.


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