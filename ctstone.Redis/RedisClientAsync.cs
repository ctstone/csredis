using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    /// <summary>
    /// Asynchronous Redis client
    /// </summary>
    public class RedisClientAsync : IDisposable
    {
        private readonly RedisConnection _connection;
        private string _password;
        private Lazy<RedisSubscriptionClient> _subscriptionClient;
        private ActivityTracer _activity;

        /// <summary>
        /// Get a value indicating that the RedisClientAsync connection is open
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get host that the current RedisClientAsync is connected to
        /// </summary>
        public string Host { get { return _connection.Host; } }

        /// <summary>
        /// Get the port that the current RedisClientAsync is connected to
        /// </summary>
        public int Port { get { return _connection.Port; } }

        /// <summary>
        /// Get a thread-safe, reusable subscription channel.
        /// </summary>
        public RedisSubscriptionClient SubscriptionChannel { get { return _subscriptionClient.Value; } } // TODO: check _subscriptionClient.Value.Connected

        /// <summary>
        /// Occurs when a Task exception is thrown
        /// </summary>
        public event UnhandledExceptionEventHandler ExceptionOccurred;

        /// <summary>
        /// Instantiate a new instance of the RedisClientAsync class
        /// </summary>
        /// <param name="host">Redis host</param>
        /// <param name="port">Redis port</param>
        /// <param name="timeoutMilliseconds">Connection timeout in milliseconds (0 for no timeout)</param>
        public RedisClientAsync(string host, int port, int timeoutMilliseconds)
        {
            _activity = new ActivityTracer("New Redis async client");
            _connection = new RedisConnection(host, port);
            _connection.TaskReadExceptionOccurred += OnAsyncExceptionOccurred;
            _connection.Connect(timeoutMilliseconds);
            _subscriptionClient = new Lazy<RedisSubscriptionClient>(() => new RedisSubscriptionClient(Host, Port, _password));
        }

        /// <summary>
        /// Get a synchronous RedisClient for blocking calls (e.g. BLPop, Subscriptions, Transactions, etc)
        /// </summary>
        /// <returns>RedisClient to be used in single thread context</returns>
        public RedisClient Clone()
        {
            ActivityTracer.Verbose("Cloning client");
            RedisClient client = new RedisClient(Host, Port, 0);
            if (_password != null)
                client.Auth(_password);
            return client;
        }

        /// <summary>
        /// Get a thread-safe, reusable subscription channel.
        /// </summary>
        /// <returns>A reusable subscription channel</returns>
        [Obsolete("Use SubscriptionClient property instead")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public RedisSubscriptionClient GetSubscriptionChannel()
        {
            if (!_subscriptionClient.Value.Connected)
                throw new InvalidOperationException("RedisSubscriptionClient has already been disposed");
            return _subscriptionClient.Value;
        }

        /// <summary>
        /// Close the subscription channel if it is not already Disposed. The channel will be made unusable for the remainder of the current RedisClientAsync.
        /// </summary>
        public void CloseSubscriptionChannel()
        {
            if (_subscriptionClient.IsValueCreated)
                _subscriptionClient.Value.Dispose();
        }

        /// <summary>
        /// Call arbitrary redis command (e.g. for a command not yet implemented in this package)
        /// </summary>
        /// <param name="command">The name of the command</param>
        /// <param name="args">Array of arguments to the command</param>
        /// <returns>Task returning Redis unified response</returns>
        public Task<object> Call(string command, params string[] args)
        {
            return Write(new RedisObject(command, args));
        }

        /// <summary>
        /// Block the current thread and wait for the given Redis command to complete
        /// </summary>
        /// <typeparam name="T">Redis command return type</typeparam>
        /// <param name="func">Redis command method</param>
        /// <returns>Redis command output</returns>
        public T Wait<T>(Func<RedisClientAsync, Task<T>> func)
        {
            Task<T> task = func(this);
            task.Wait();
            return task.Result;
        }

        #region Connection
        /// <summary>
        /// Authenticate to the server
        /// </summary>
        /// <param name="password">Server password</param>
        /// <returns>Task associated with status message</returns>
        public Task<string> Auth(string password)
        {
            _password = password;
            return Write(RedisCommand.Auth(password));
        }

        /// <summary>
        /// Echo the given string
        /// </summary>
        /// <param name="message">Message to echo</param>
        /// <returns>Task associated with echo response</returns>
        public Task<string> Echo(string message)
        {
            return Write(RedisCommand.Echo(message));
        }

        /// <summary>
        /// Ping the server
        /// </summary>
        /// <returns>Task associated with status message</returns>
        public Task<string> Ping()
        {
            return Write(RedisCommand.Ping());
        }

        /// <summary>
        /// Close the connection
        /// </summary>
        /// <returns>Task associated with status message</returns>
        public Task<string> Quit()
        {
            return Write(RedisCommand.Quit())
                .ContinueWith<string>(t =>
                {
                    _connection.Dispose();
                    return t.Result;
                });
        }
        #endregion

        #region Keys
        /// <summary>
        /// Delete a key
        /// </summary>
        /// <param name="keys">Keys to delete</param>
        /// <returns></returns>
        public Task<long> Del(params string[] keys)
        {
            return Write(RedisCommand.Del(keys));
        }

        /// <summary>
        /// Return a serialized version of the value stored at the specified key
        /// </summary>
        /// <param name="key">Key to dump</param>
        /// <returns></returns>
        public Task<byte[]> Dump(string key)
        {
            return Write(RedisCommand.Dump(key));
        }

        /// <summary>
        /// Determine if a key exists
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<bool> Exists(string key)
        {
            return Write(RedisCommand.Exists(key));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest second)</param>
        /// <returns></returns>
        public Task<bool> Expire(string key, int expiration)
        {
            return Write(RedisCommand.Expire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration in seconds</param>
        /// <returns></returns>
        public Task<bool> Expire(string key, TimeSpan expiration)
        {
            return Write(RedisCommand.Expire(key, expiration));
        }

        /// <summary>
        /// Set the expiration for a key (nearest second)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expirationDate">Date of expiration, to nearest second</param>
        /// <returns></returns>
        public Task<bool> ExpireAt(string key, DateTime expirationDate)
        {
            return Write(RedisCommand.ExpireAt(key, expirationDate));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public Task<bool> ExpireAt(string key, int timestamp)
        {
            return Write(RedisCommand.ExpireAt(key, timestamp));
        }

        /// <summary>
        /// Find all keys matching the given pattern
        /// </summary>
        /// <param name="pattern">Pattern to match</param>
        /// <returns></returns>
        public Task<string[]> Keys(string pattern)
        {
            return Write(RedisCommand.Keys(pattern));
        }

        /// <summary>
        /// Atomically transfer a key from a Redis instance to another one
        /// </summary>
        /// <param name="host">Remote Redis host</param>
        /// <param name="port">Remote Redis port</param>
        /// <param name="key">Key to migrate</param>
        /// <param name="destinationDb">Remote database ID</param>
        /// <param name="timeout">Timeout in milliseconds</param>
        /// <returns></returns>
        public Task<string> Migrate(string host, int port, string key, int destinationDb, int timeout)
        {
            return Write(RedisCommand.Migrate(host, port, key, destinationDb, timeout));
        }

        /// <summary>
        /// Atomically transfer a key from a Redis instance to another one
        /// </summary>
        /// <param name="host">Remote Redis host</param>
        /// <param name="port">Remote Redis port</param>
        /// <param name="key">Key to migrate</param>
        /// <param name="destinationDb">Remote database ID</param>
        /// <param name="timeout">Timeout in milliseconds</param>
        /// <returns></returns>
        public Task<string> Migrate(string host, int port, string key, int destinationDb, TimeSpan timeout)
        {
            return Write(RedisCommand.Migrate(host, port, key, destinationDb, timeout));
        }

        /// <summary>
        /// Move a key to another database
        /// </summary>
        /// <param name="key">Key to move</param>
        /// <param name="database">Database destination ID</param>
        /// <returns></returns>
        public Task<bool> Move(string key, int database)
        {
            return Write(RedisCommand.Move(key, database));
        }

        /// <summary>
        /// Get the number of references of the value associated with the specified key
        /// </summary>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>The type of internal representation used to store the value at the specified key</returns>
        public Task<string> ObjectEncoding(params string[] arguments)
        {
            return Write(RedisCommand.ObjectEncoding(arguments));
        }

        /// <summary>
        /// Inspect the internals of Redis objects
        /// </summary>
        /// <param name="subCommand">Type of Object command to send</param>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>Varies depending on subCommand</returns>
        public Task<long?> Object(RedisObjectSubCommand subCommand, params string[] arguments)
        {
            return Write(RedisCommand.Object(subCommand, arguments));
        }

        /// <summary>
        /// Remove the expiration from a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns></returns>
        public Task<bool> Persist(string key)
        {
            return Write(RedisCommand.Persist(key));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest millisecond)</param>
        /// <returns></returns>
        public Task<bool> PExpire(string key, TimeSpan expiration)
        {
            return Write(RedisCommand.PExpire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="milliseconds">Expiration in milliseconds</param>
        /// <returns></returns>
        public Task<bool> PExpire(string key, long milliseconds)
        {
            return Write(RedisCommand.PExpire(key, milliseconds));
        }

        /// <summary>
        /// Set the expiration for a key (nearest millisecond)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="date">Expiration date</param>
        /// <returns></returns>
        public Task<bool> PExpireAt(string key, DateTime date)
        {
            return Write(RedisCommand.PExpireAt(key, date));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp specified in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp">Expiration timestamp (milliseconds)</param>
        /// <returns></returns>
        public Task<bool> PExpireAt(string key, long timestamp)
        {
            return Write(RedisCommand.PExpireAt(key, timestamp));
        }

        /// <summary>
        /// Get the time to live for a key in milliseconds
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<long> PTtl(string key)
        {
            return Write(RedisCommand.PTtl(key));
        }

        /// <summary>
        /// Return a random key from the keyspace
        /// </summary>
        /// <returns></returns>
        public Task<string> RandomKey()
        {
            return Write(RedisCommand.RandomKey());
        }

        /// <summary>
        /// Rename a key
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns></returns>
        public Task<string> Rename(string key, string newKey)
        {
            return Write(RedisCommand.Rename(key, newKey));
        }

        /// <summary>
        /// Rename a key, only if the new key does not exist
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns></returns>
        public Task<bool> RenameNx(string key, string newKey)
        {
            return Write(RedisCommand.RenameNx(key, newKey));
        }

        /// <summary>
        /// Create a key using the provided serialized value, previously obtained using dump
        /// </summary>
        /// <param name="key">Key to restore</param>
        /// <param name="ttl">Time-to-live in milliseconds</param>
        /// <param name="serializedValue">Serialized value from DUMP</param>
        /// <returns></returns>
        public Task<string> Restore(string key, long ttl, string serializedValue)
        {
            return Write(RedisCommand.Restore(key, ttl, serializedValue));
        }

        /// <summary>
        /// Sort the elements in a list, set or sorted set
        /// </summary>
        /// <param name="key">Key to sort</param>
        /// <param name="offset">Number of elements to skip</param>
        /// <param name="count">Number of elements to return</param>
        /// <param name="by">Sort by external key</param>
        /// <param name="dir">Sort direction</param>
        /// <param name="isAlpha">Sort lexicographically</param>
        /// <param name="get">Retrieve external keys</param>
        /// <returns></returns>
        public Task<string[]> Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            return Write(RedisCommand.Sort(key, offset, count, by, dir, isAlpha, get));
        }

        /// <summary>
        /// Sort the elements in a list, set or sorted set, then store the result in a new list
        /// </summary>
        /// <param name="key">Key to sort</param>
        /// <param name="destination">Destination key name of stored sort</param>
        /// <param name="offset">Number of elements to skip</param>
        /// <param name="count">Number of elements to return</param>
        /// <param name="by">Sort by external key</param>
        /// <param name="dir">Sort direction</param>
        /// <param name="isAlpha">Sort lexicographically</param>
        /// <param name="get">Retrieve external keys</param>
        /// <returns></returns>
        public Task<long> SortAndStore(string key, string destination, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            return Write(RedisCommand.SortAndStore(key, destination, offset, count, by, dir, isAlpha, get));
        }

        /// <summary>
        /// Get the time to live for a key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<long> Ttl(string key)
        {
            return Write(RedisCommand.Ttl(key));
        }

        /// <summary>
        /// Determine the type stored at key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<string> Type(string key)
        {
            return Write(RedisCommand.Type(key));
        }
        #endregion

        #region Hashes
        /// <summary>
        /// Delete one or more hash fields
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="fields">Fields to delete</param>
        /// <returns>Number of fields removed from hash</returns>
        public Task<long> HDel(string key, params string[] fields)
        {
            return Write(RedisCommand.HDel(key, fields));
        }

        /// <summary>
        /// Determine if a hash field exists
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to check</param>
        /// <returns>True if hash field exists</returns>
        public Task<bool> HExists(string key, string field)
        {
            return Write(RedisCommand.HExists(key, field));
        }

        /// <summary>
        /// Get the value of a hash field
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to get</param>
        /// <returns>Value of hash field</returns>
        public Task<string> HGet(string key, string field)
        {
            return Write(RedisCommand.HGet(key, field));
        }
        
        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <typeparam name="T">Object to map hash</typeparam>
        /// <param name="key">Hash key</param>
        /// <returns>Strongly typed object mapped from hash</returns>
        public Task<T> HGetAll<T>(string key)
            where T : class
        {
            return Write(RedisCommand.HGetAll<T>(key));
        }
        
        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Dictionary mapped from string</returns>
        public Task<Dictionary<string, string>> HGetAll(string key)
        {
            return Write(RedisCommand.HGetAll(key));
        }

        /// <summary>
        /// Increment the integer value of a hash field by the given number
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to increment</param>
        /// <param name="increment">Increment value</param>
        /// <returns>Value of field after increment</returns>
        public Task<long> HIncrBy(string key, string field, long increment)
        {
            return Write(RedisCommand.HIncrBy(key, field, increment));
        }

        /// <summary>
        /// Increment the float value of a hash field by the given number
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to increment</param>
        /// <param name="increment">Increment value</param>
        /// <returns>Value of field after increment</returns>
        public Task<double> HIncrByFloat(string key, string field, double increment)
        {
            return Write(RedisCommand.HIncrByFloat(key, field, increment));
        }

        /// <summary>
        /// Get all the fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>All hash field names</returns>
        public Task<string[]> HKeys(string key)
        {
            return Write(RedisCommand.HKeys(key));
        }

        /// <summary>
        /// Get the number of fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Number of fields in hash</returns>
        public Task<long> HLen(string key)
        {
            return Write(RedisCommand.HLen(key));
        }

        /// <summary>
        /// Get the values of all the given hash fields
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="fields">Fields to return</param>
        /// <returns>Values of given fields</returns>
        public Task<string[]> HMGet(string key, params string[] fields)
        {
            return Write(RedisCommand.HMGet(key, fields));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="dict">Dictionary mapping of hash</param>
        /// <returns>Status code</returns>
        public Task<string> HMSet(string key, Dictionary<string, string> dict)
        {
            return Write(RedisCommand.HMSet(key, dict));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <typeparam name="T">Type of object to map hash</typeparam>
        /// <param name="key">Hash key</param>
        /// <param name="obj">Object mapping of hash</param>
        /// <returns>Status code</returns>
        public Task<string> HMSet<T>(string key, T obj)
            where T : class
        {
            return Write(RedisCommand.HMSet<T>(key, obj));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="keyValues">Array of [key,value,key,value,..]</param>
        /// <returns>Status code</returns>
        public Task<string> HMSet(string key, params string[] keyValues)
        {
            return Write(RedisCommand.HMSet(key, keyValues));
        }

        /// <summary>
        /// Set the value of a hash field
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Hash field to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if field is new</returns>
        public Task<bool> HSet(string key, string field, object value)
        {
            return Write(RedisCommand.HSet(key, field, value));
        }

        /// <summary>
        /// Set the value of a hash field, only if the field does not exist
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Hash field to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if field was set to value</returns>
        public Task<bool> HSetNx(string key, string field, object value)
        {
            return Write(RedisCommand.HSetNx(key, field, value));
        }

        /// <summary>
        /// Get all the values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Array of all values in hash</returns>
        public Task<string[]> HVals(string key)
        {
            return Write(RedisCommand.HVals(key));
        }
        #endregion

        #region Lists
        /// <summary>
        /// Get an element from a list by its index
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="index">Zero-based index of item to return</param>
        /// <returns>Element at index</returns>
        public Task<string> LIndex(string key, long index)
        {
            return Write(RedisCommand.LIndex(key, index));
        }

        /// <summary>
        /// Insert an element before or after another element in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="insertType">Relative position</param>
        /// <param name="pivot">Relative element</param>
        /// <param name="value">Element to insert</param>
        /// <returns>Length of list after insert or -1 if pivot not found</returns>
        public Task<long> LInsert(string key, RedisInsert insertType, string pivot, object value)
        {
            return Write(RedisCommand.LInsert(key, insertType, pivot, value));
        }

        /// <summary>
        /// Get the length of a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Length of list at key</returns>
        public Task<long> LLen(string key)
        {
            return Write(RedisCommand.LLen(key));
        }

        /// <summary>
        /// Remove and get the first element in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>First element in list</returns>
        public Task<string> LPop(string key)
        {
            return Write(RedisCommand.LPop(key));
        }

        /// <summary>
        /// Prepend one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> LPush(string key, params object[] values)
        {
            return Write(RedisCommand.LPush(key, values));
        }

        /// <summary>
        /// Prepend a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="value">Value to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> LPushX(string key, object value)
        {
            return Write(RedisCommand.LPushX(key, value));
        }

        /// <summary>
        /// Get a range of elements from a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <returns>List of elements in range</returns>
        public Task<string[]> LRange(string key, long start, long stop)
        {
            return Write(RedisCommand.LRange(key, start, stop));
        }

        /// <summary>
        /// Remove elements from a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="count">&gt;0: remove N elements from head to tail; &lt;0: remove N elements from tail to head; =0: remove all elements</param>
        /// <param name="value">Remove elements equal to value</param>
        /// <returns>Number of removed elements</returns>
        public Task<long> LRem(string key, long count, object value)
        {
            return Write(RedisCommand.LRem(key, count, value));
        }

        /// <summary>
        /// Set the value of an element in a list by its index
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="index">List index to modify</param>
        /// <param name="value">New element value</param>
        /// <returns>Status code</returns>
        public Task<string> LSet(string key, long index, object value)
        {
            return Write(RedisCommand.LSet(key, index, value));
        }

        /// <summary>
        /// Trim a list to the specified range
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="start">Zero-based start index</param>
        /// <param name="stop">Zero-based stop index</param>
        /// <returns>Status code</returns>
        public Task<string> LTrim(string key, long start, long stop)
        {
            return Write(RedisCommand.LTrim(key, start, stop));
        }

        /// <summary>
        /// Remove and get the last elment in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Value of last list element</returns>
        public Task<string> RPop(string key)
        {
            return Write(RedisCommand.RPop(key));
        }

        /// <summary>
        /// Remove the last elment in a list, append it to another list and return it
        /// </summary>
        /// <param name="source">List source key</param>
        /// <param name="destination">Destination key</param>
        /// <returns>Element being popped and pushed</returns>
        public Task<string> RPopLPush(string source, string destination)
        {
            return Write(RedisCommand.RPopLPush(source, destination));
        }

        /// <summary>
        /// Append one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> RPush(string key, params object[] values)
        {
            return Write(RedisCommand.RPush(key, values));
        }

        /// <summary>
        /// Append a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> RPushX(string key, params object[] values)
        {
            return Write(RedisCommand.RPushX(key, values));
        }
        #endregion

        #region Sets
        /// <summary>
        /// Add one or more members to a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="members">Members to add to set</param>
        /// <returns>Number of elements added to set</returns>
        public Task<long> SAdd(string key, params object[] members)
        {
            return Write(RedisCommand.SAdd(key, members));
        }

        /// <summary>
        /// Get the number of members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>Number of elements in set</returns>
        public Task<long> SCard(string key)
        {
            return Write(RedisCommand.SCard(key));
        }

        /// <summary>
        /// Subtract multiple sets
        /// </summary>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SDiff(params string[] keys)
        {
            return Write(RedisCommand.SDiff(keys));
        }

        /// <summary>
        /// Subtract multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Number of elements in the resulting set</returns>
        public Task<long> SDiffStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SDiffStore(destination, keys));
        }

        /// <summary>
        /// Intersect multiple sets
        /// </summary>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SInter(params string[] keys)
        {
            return Write(RedisCommand.SInter(keys));
        }

        /// <summary>
        /// Intersect multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Number of elements in resulting set</returns>
        public Task<long> SInterStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SInterStore(destination, keys));
        }

        /// <summary>
        /// Determine if a given value is a member of a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>True if member exists in set</returns>
        public Task<bool> SIsMember(string key, object member)
        {
            return Write(RedisCommand.SIsMember(key, member));
        }

        /// <summary>
        /// Get all the members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>All elements in the set</returns>
        public Task<string[]> SMembers(string key)
        {
            return Write(RedisCommand.SMembers(key));
        }

        /// <summary>
        /// Move a member from one set to another
        /// </summary>
        /// <param name="source">Source key</param>
        /// <param name="destination">Destination key</param>
        /// <param name="member">Member to move</param>
        /// <returns>True if element was moved</returns>
        public Task<bool> SMove(string source, string destination, object member)
        {
            return Write(RedisCommand.SMove(source, destination, member));
        }

        /// <summary>
        /// Remove and return a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>The removed element</returns>
        public Task<string> SPop(string key)
        {
            return Write(RedisCommand.SPop(key));
        }

        /// <summary>
        /// Get a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>One random element from set</returns>
        public Task<string> SRandMember(string key)
        {
            return Write(RedisCommand.SRandMember(key));
        }

        /// <summary>
        /// Get one or more random members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>One or more random elements from set</returns>
        public Task<string[]> SRandMember(string key, long count)
        {
            return Write(RedisCommand.SRandMember(key, count));
        }

        /// <summary>
        /// Remove one or more members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="members">Set members to remove</param>
        /// <returns>Number of elements removed from set</returns>
        public Task<long> SRem(string key, params object[] members)
        {
            return Write(RedisCommand.SRem(key, members));
        }

        /// <summary>
        /// Add multiple sets
        /// </summary>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SUnion(params string[] keys)
        {
            return Write(RedisCommand.SUnion(keys));
        }

        /// <summary>
        /// Add multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Number of elements in resulting set</returns>
        public Task<long> SUnionStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SUnionStore(destination, keys));
        }
        #endregion

        #region Sorted Sets
        /// <summary>
        /// Add one or more members to a sorted set, or update its score if it already exists
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="memberScores">Array of member scores to add to sorted set</param>
        /// <returns>Number of elements added to the sorted set (not including member updates)</returns>
        public Task<long> ZAdd<TScore, TMember>(string key, params Tuple<TScore, TMember>[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Add one or more members to a sorted set, or update its score if it already exists
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="memberScores">Array of member scores [s1, m1, s2, m2, ..]</param>
        /// <returns>Number of elements added to the sorted set (not including member updates)</returns>
        public Task<long> ZAdd(string key, params string[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Get the number of members in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <returns>Number of elements in the sorted set</returns>
        public Task<long> ZCard(string key)
        {
            return Write(RedisCommand.ZCard(key));
        }

        /// <summary>
        /// Count the members in a sorted set with scores within the given values
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="exclusiveMin">Minimum score is exclusive</param>
        /// <param name="exclusiveMax">Maximum score is exclusive</param>
        /// <returns>Number of elements in the specified score range</returns>
        public Task<long> ZCount(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return Write(RedisCommand.ZCount(key, min, max, exclusiveMin, exclusiveMax));
        }

        /// <summary>
        /// Increment the score of a member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="increment">Increment by value</param>
        /// <param name="member">Sorted set member to increment</param>
        /// <returns>New score of member</returns>
        public Task<double> ZIncrBy(string key, double increment, string member)
        {
            return Write(RedisCommand.ZIncrBy(key, increment, member));
        }

        /// <summary>
        /// Intersect multiple sorted sets and store the resulting set in a new key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="weights">Multiplication factor for each input set</param>
        /// <param name="aggregate">Aggregation function of resulting set</param>
        /// <param name="keys">Sorted set keys to intersect</param>
        /// <returns>Number of elements in the resulting sorted set</returns>
        public Task<long> ZInterStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return Write(RedisCommand.ZInterStore(destination, weights, aggregate, keys));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <param name="withScores">Include scores in result</param>
        /// <returns>Array of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRange(string key, long start, long stop, bool withScores = false)
        {
            return Write(RedisCommand.ZRange(key, start, stop, withScores));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="withScores">Include scores in result</param>
        /// <param name="exclusiveMin">Minimum score is exclusive</param>
        /// <param name="exclusiveMax">Maximum score is exclusive</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRangeByScore(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRangeByScore(key, min, max, withScores, exclusiveMin, exclusiveMax, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member or null if key does not exist</returns>
        public Task<long?> ZRank(string key, string member)
        {
            return Write(RedisCommand.ZRank(key, member));
        }

        /// <summary>
        /// Remove one or more members from a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="members">Members to remove</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRem(string key, params object[] members)
        {
            return Write(RedisCommand.ZRem(key, members));
        }

        /// <summary>
        /// Remove all members in a sorted set within the given indexes
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemRangeByRank(string key, long start, long stop)
        {
            return Write(RedisCommand.ZRemRangeByRank(key, start, stop));
        }

        /// <summary>
        /// Remove all members in a sorted set within the given scores
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="exclusiveMin">Minimum score is exclusive</param>
        /// <param name="exclusiveMax">Maximum score is exclusive</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemRangeByScore(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return Write(RedisCommand.ZRemRangeByScore(key, min, max, exclusiveMin, exclusiveMax));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <param name="withScores">Include scores in result</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRevRange(string key, long start, long stop, bool withScores = false)
        {
            return Write(RedisCommand.ZRevRange(key, start, stop, withScores));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="max">Maximum score</param>
        /// <param name="min">Minimum score</param>
        /// <param name="withScores">Include scores in result</param>
        /// <param name="exclusiveMax">Maximum score is exclusive</param>
        /// <param name="exclusiveMin">Minimum score is exclusive</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified score range (with optional scores)</returns>
        public Task<string[]> ZRevRangeByScore(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRevRangeByScore(key, max, min, withScores, exclusiveMax, exclusiveMin, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member, or null if member does not exist</returns>
        public Task<long?> ZRevRank(string key, string member)
        {
            return Write(RedisCommand.ZRevRank(key, member));
        }

        /// <summary>
        /// Get the score associated with the given member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Score of member, or null if member does not exist</returns>
        public Task<double?> ZScore(string key, string member)
        {
            return Write(RedisCommand.ZScore(key, member));
        }

        /// <summary>
        /// Add multiple sorted sets and store the resulting sorted set in a new key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="weights">Multiplication factor for each input set</param>
        /// <param name="aggregate">Aggregation function of resulting set</param>
        /// <param name="keys">Sorted set keys to union</param>
        /// <returns>Number of elements in the resulting sorted set</returns>
        public Task<long> ZUnionStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return Write(RedisCommand.ZUnionStore(destination, weights, aggregate, keys));
        }

        /// <summary>
        /// Iterate the scores and elements of a sorted set field
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="cursor">The cursor returned by the server in the previous call, or 0 if this is the first call</param>
        /// <param name="pattern">Glob-style pattern to filter returned elements</param>
        /// <param name="count">Maximum number of elements to return</param>
        /// <returns>Updated cursor and result set</returns>
        public Task<RedisScanPair> ZScan(string key, long cursor, string pattern = null, long? count = null)
        {
            return Write(RedisCommand.ZScan(key, cursor, pattern, count));
        }

        /// <summary>
        /// Retrieve all the elements in a sorted set with a value between min and max
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Lexagraphic start value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="max">Lexagraphic stop value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="offset">Limit result set by offset</param>
        /// <param name="count">Limimt result set by size</param>
        /// <returns>List of elements in the specified range</returns>
        public Task<string[]> ZRangeByLex(string key, string min, string max, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRangeByLex(key, min, max, offset, count));
        }

        /// <summary>
        /// Remove all elements in the sorted set with a value between min and max
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Lexagraphic start value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="max">Lexagraphic stop value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemRangeByLex(string key, string min, string max)
        {
            return Write(RedisCommand.ZRemRangeByLex(key, min, max));
        }

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Lexagraphic start value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="max">Lexagraphic stop value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <returns>Number of elements in the specified score range</returns>
        public Task<long> ZLexCount(string key, string min, string max)
        {
            return Write(RedisCommand.ZLexCount(key, min, max));
        }
        #endregion

        #region Pub/Sub
        /// <summary>
        /// Post a message to a channel
        /// </summary>
        /// <param name="channel">Channel to post message</param>
        /// <param name="message">Message to send</param>
        /// <returns>Number of clients that received the message</returns>
        public Task<long> Publish(string channel, string message)
        {
            return Write(RedisCommand.Publish(channel, message));
        }
        #endregion

        #region Scripting
        /// <summary>
        /// Execute a Lua script server side
        /// </summary>
        /// <param name="script">Script to run on server</param>
        /// <param name="keys">Keys used by script</param>
        /// <param name="arguments">Arguments to pass to script</param>
        /// <returns>Redis object</returns>
        public Task<object> Eval(string script, string[] keys, params string[] arguments)
        {
            return Write(RedisCommand.Eval(script, keys, arguments));
        }

        /// <summary>
        /// Execute a Lua script server side, sending only the script's cached SHA hash
        /// </summary>
        /// <param name="sha1">SHA1 hash of script</param>
        /// <param name="keys">Keys used by script</param>
        /// <param name="arguments">Arguments to pass to script</param>
        /// <returns>Redis object</returns>
        public Task<object> EvalSHA(string sha1, string[] keys, params string[] arguments)
        {
            return Write(RedisCommand.EvalSHA(sha1, keys, arguments));
        }

        /// <summary>
        /// Check existence of script SHA hashes in the script cache
        /// </summary>
        /// <param name="scripts">SHA1 script hashes</param>
        /// <returns>Array of boolean values indicating script existence on server</returns>
        public Task<bool[]> ScriptExists(params string[] scripts)
        {
            return Write(RedisCommand.ScriptExists(scripts));
        }

        /// <summary>
        /// Remove all scripts from the script cache
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ScriptFlush()
        {
            return Write(RedisCommand.ScriptFlush());
        }

        /// <summary>
        /// Kill the script currently in execution
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ScriptKill()
        {
            return Write(RedisCommand.ScriptKill());
        }

        /// <summary>
        /// Load the specified Lua script into the script cache
        /// </summary>
        /// <param name="script">Lua script to load</param>
        /// <returns>SHA1 hash of script</returns>
        public Task<string> ScriptLoad(string script)
        {
            return Write(RedisCommand.ScriptLoad(script));
        }
        #endregion

        #region Strings
        /// <summary>
        /// Append a value to a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to append to key</param>
        /// <returns>Length of string after append</returns>
        public Task<long> Append(string key, object value)
        {
            return Write(RedisCommand.Append(key, value));
        }

        /// <summary>
        /// Count set bits in a string
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <param name="start">Start offset</param>
        /// <param name="end">Stop offset</param>
        /// <returns>Number of bits set to 1</returns>
        public Task<long> BitCount(string key, long? start = null, long? end = null)
        {
            return Write(RedisCommand.BitCount(key, start, end));
        }

        /// <summary>
        /// Perform bitwise operations between strings
        /// </summary>
        /// <param name="operation">Bit command to execute</param>
        /// <param name="destKey">Store result in destination key</param>
        /// <param name="keys">Keys to operate</param>
        /// <returns>Size of string stored in the destination key</returns>
        public Task<long> BitOp(RedisBitOp operation, string destKey, params string[] keys)
        {
            return Write(RedisCommand.BitOp(operation, destKey, keys));
        }

        /// <summary>
        /// Find first bit set or clear in a string
        /// </summary>
        /// <param name="key">Key to examine</param>
        /// <param name="bit">Bit value (1 or 0)</param>
        /// <param name="start">Examine string at specified byte offset</param>
        /// <param name="end">Examine string to specified byte offset</param>
        /// <returns>Position of the first bit set to the specified value</returns>
        public Task<long> BitPos(string key, byte bit, long? start = null, long? end = null)
        {
            return Write(RedisCommand.BitPos(key, bit, start, end));
        }

        /// <summary>
        /// Decrement the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after decrement</returns>
        public Task<long> Decr(string key)
        {
            return Write(RedisCommand.Decr(key));
        }

        /// <summary>
        /// Decrement the integer value of a key by the given number
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="decrement">Decrement value</param>
        /// <returns>Value of key after decrement</returns>
        public Task<long> DecrBy(string key, long decrement)
        {
            return Write(RedisCommand.DecrBy(key, decrement));
        }

        /// <summary>
        /// Get the value of a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Value of key</returns>
        public Task<string> Get(string key)
        {
            return Write(RedisCommand.Get(key));
        }

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <param name="offset">Offset of key to check</param>
        /// <returns>Bit value stored at offset</returns>
        public Task<bool> GetBit(string key, uint offset)
        {
            return Write(RedisCommand.GetBit(key, offset));
        }

        /// <summary>
        /// Get a substring of the string stored at a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <param name="start">Start offset</param>
        /// <param name="end">End offset</param>
        /// <returns>Substring in the specified range</returns>
        public Task<string> GetRange(string key, long start, long end)
        {
            return Write(RedisCommand.GetRange(key, start, end));
        }

        /// <summary>
        /// Set the string value of a key and return its old value
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Old value stored at key, or null if key did not exist</returns>
        public Task<string> GetSet(string key, object value)
        {
            return Write(RedisCommand.GetSet(key, value));
        }

        /// <summary>
        /// Increment the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after increment</returns>
        public Task<long> Incr(string key)
        {
            return Write(RedisCommand.Incr(key));
        }

        /// <summary>
        /// Increment the integer value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public Task<long> IncrBy(string key, long increment)
        {
            return Write(RedisCommand.IncrBy(key, increment));
        }

        /// <summary>
        /// Increment the float value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public Task<double> IncrByFloat(string key, double increment)
        {
            return Write(RedisCommand.IncrByFloat(key, increment));
        }

        /// <summary>
        /// Get the values of all the given keys
        /// </summary>
        /// <param name="keys">Keys to lookup</param>
        /// <returns>Array of values at the specified keys</returns>
        public Task<string[]> MGet(params string[] keys)
        {
            return Write(RedisCommand.MGet(keys));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>Status code</returns>
        public Task<string> MSet(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>Status code</returns>
        public Task<string> MSet(params string[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>True if all keys were set</returns>
        public Task<bool> MSetNx(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSetNx(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>True if all keys were set</returns>
        public Task<bool> MSetNx(params string[] keyValues)
        {
            return Write(RedisCommand.MSetNx(keyValues));
        }

        /// <summary>
        /// Set the value and expiration in milliseconds of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="milliseconds">Expiration in milliseconds</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> PSetEx(string key, long milliseconds, object value)
        {
            return Write(RedisCommand.PSetEx(key, milliseconds, value));
        }

        /// <summary>
        /// Set the string value of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> Set(string key, object value)
        {
            return Write(RedisCommand.Set(key, value));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expiration">Set expiration to nearest millisecond</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> Set(string key, object value, TimeSpan expiration, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expiration, condition));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expirationSeconds">Set expiration to nearest second</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> Set(string key, object value, int? expirationSeconds = null, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expirationSeconds, condition));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expirationMilliseconds">Set expiration to nearest millisecond</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> Set(string key, object value, long? expirationMilliseconds = null, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expirationMilliseconds, condition));
        }

        /// <summary>
        /// Sets or clears the bit at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="offset">Modify key at offset</param>
        /// <param name="value">Value to set (on or off)</param>
        /// <returns>Original bit stored at offset</returns>
        public Task<bool> SetBit(string key, uint offset, bool value)
        {
            return Write(RedisCommand.SetBit(key, offset, value));
        }

        /// <summary>
        /// Set the value and expiration of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="seconds">Expiration in seconds</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> SetEx(string key, long seconds, object value)
        {
            return Write(RedisCommand.SetEx(key, seconds, value));
        }

        /// <summary>
        /// Set the value of a key, only if the key does not exist
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if key was set</returns>
        public Task<bool> SetNx(string key, object value)
        {
            return Write(RedisCommand.SetNx(key, value));
        }

        /// <summary>
        /// Overwrite part of a string at key starting at the specified offset
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="offset">Start offset</param>
        /// <param name="value">Value to write at offset</param>
        /// <returns>Length of string after operation</returns>
        public Task<long> SetRange(string key, uint offset, object value)
        {
            return Write(RedisCommand.SetRange(key, offset, value));
        }

        /// <summary>
        /// Get the length of the value stored in a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Length of string at key</returns>
        public Task<long> StrLen(string key)
        {
            return Write(RedisCommand.StrLen(key));
        }
        #endregion

        #region Server
        /// <summary>
        /// Asyncronously rewrite the append-only file
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> BgRewriteAof()
        {
            return Write(RedisCommand.BgRewriteAof());
        }

        /// <summary>
        /// Asynchronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> BgSave()
        {
            return Write(RedisCommand.BgSave());
        }

        /// <summary>
        /// Get the current connection name
        /// </summary>
        /// <returns>Connection name</returns>
        public Task<string> ClientGetName()
        {
            return Write(RedisCommand.ClientGetName());
        }

        /// <summary>
        /// Kill the connection of a client
        /// </summary>
        /// <param name="ip">Client IP returned from CLIENT LIST</param>
        /// <param name="port">Client port returned from CLIENT LIST</param>
        /// <returns>Status code</returns>
        public Task<string> ClientKill(string ip, int port)
        {
            return Write(RedisCommand.ClientKill(ip, port));
        }

        /// <summary>
        /// Get the list of client connections
        /// </summary>
        /// <returns>Formatted string of clients</returns>
        public Task<string> ClientList()
        {
            return Write(RedisCommand.ClientList());
        }

        /// <summary>
        /// Set the current connection name
        /// </summary>
        /// <param name="connectionName">Name of connection (no spaces)</param>
        /// <returns>Status code</returns>
        public Task<string> ClientSetName(string connectionName)
        {
            return Write(RedisCommand.ClientSetName(connectionName));
        }

        /// <summary>
        /// Get the value of a configuration paramter
        /// </summary>
        /// <param name="parameter">Configuration parameter to lookup</param>
        /// <returns>Configuration value</returns>
        public Task<string> ConfigGet(string parameter)
        {
            return Write(RedisCommand.ConfigGet(parameter));
        }

        /// <summary>
        /// Reset the stats returned by INFO
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ConfigResetStat()
        {
            return Write(RedisCommand.ConfigResetStat());
        }

        /// <summary>
        /// Set a configuration parameter to the given value
        /// </summary>
        /// <param name="parameter">Parameter to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> ConfigSet(string parameter, string value)
        {
            return Write(RedisCommand.ConfigSet(parameter, value));
        }

        /// <summary>
        /// Return the number of keys in the selected database
        /// </summary>
        /// <returns>Number of keys</returns>
        public Task<long> DbSize()
        {
            return Write(RedisCommand.DbSize());
        }

        /// <summary>
        /// Get debugging information about a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Status code</returns>
        public Task<string> DebugObject(string key)
        {
            return Write(RedisCommand.DebugObject(key));
        }

        /// <summary>
        /// Make the server crash :(
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> DebugSegFault()
        {
            return Write(RedisCommand.DebugSegFault());
        }

        /// <summary>
        /// Remove all keys from all databases
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> FlushAll()
        {
            return Write(RedisCommand.FlushAll());
        }

        /// <summary>
        /// Remove all keys from the current database
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> FlushDb()
        {
            return Write(RedisCommand.FlushDb());
        }

        /// <summary>
        /// Get information and statistics about the server
        /// </summary>
        /// <param name="section">all|default|server|clients|memory|persistence|stats|replication|cpu|commandstats|cluster|keyspace</param>
        /// <returns>Formatted string</returns>
        public Task<string> Info(string section = null)
        {
            return Write(RedisCommand.Info());
        }

        /// <summary>
        /// Get the timestamp of the last successful save to disk
        /// </summary>
        /// <returns>Date of last save</returns>
        public Task<DateTime> LastSave()
        {
            return Write(RedisCommand.LastSave());
        }

        /// <summary>
        /// Syncronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> Save()
        {
            return Write(RedisCommand.Save());
        }

        /// <summary>
        /// Syncronously save the dataset to disk an then shut down the server
        /// </summary>
        /// <param name="save">Force a DB saving operation even if no save points are configured</param>
        /// <returns>Status code</returns>
        public Task<string> Shutdown(bool? save = null)
        {
            return Write(RedisCommand.Shutdown());
        }

        /// <summary>
        /// Make the server a slave of another instance or promote it as master
        /// </summary>
        /// <param name="host">Master host</param>
        /// <param name="port">master port</param>
        /// <returns>Status code</returns>
        public Task<string> SlaveOf(string host, int port)
        {
            return Write(RedisCommand.SlaveOf(host, port));
        }

        /// <summary>
        /// Turn off replication, turning the Redis server into a master
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> SlaveOfNoOne()
        {
            return Write(RedisCommand.SlaveOfNoOne());
        }

        /// <summary>
        /// Manges the Redis slow queries log
        /// </summary>
        /// <param name="subCommand">Slowlog sub-command</param>
        /// <param name="argument">Optional argument to sub-command</param>
        /// <returns>Redis unified object</returns>
        public Task<object> SlowLog(RedisSlowLog subCommand, string argument = null)
        {
            return Write(RedisCommand.SlowLog(subCommand, argument));
        }

        /// <summary>
        /// Internal command used for replication
        /// </summary>
        /// <returns>Byte array of Redis sync data</returns>
        public Task<byte[]> Sync()
        {
            return Write(RedisCommand.Sync());
        }

        /// <summary>
        /// Return the current server time
        /// </summary>
        /// <returns>Server time</returns>
        public Task<DateTime> Time()
        {
            return Write(RedisCommand.Time());
        }
        #endregion

        #region HyperLogLog
        /// <summary>
        /// Adds the specified elements to the specified HyperLogLog.
        /// </summary>
        /// <param name="key">Key to update</param>
        /// <param name="elements">Elements to add</param>
        /// <returns>1 if at least 1 HyperLogLog internal register was altered. 0 otherwise.</returns>
        public Task<long> PfAdd(string key, params object[] elements)
        {
            return Write(RedisCommand.PfAdd(key, elements));
        }
        /// <summary>
        /// Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s)
        /// </summary>
        /// <param name="keys">One or more HyperLogLog keys to examine</param>
        /// <returns>Approximated number of unique elements observed via PFADD</returns>
        public Task<long> PfCount(params string[] keys)
        {
            return Write(RedisCommand.PfCount(keys));
        }
        /// <summary>
        /// Merge N different HyperLogLogs into a single key.
        /// </summary>
        /// <param name="destKey">Where to store the merged HyperLogLogs</param>
        /// <param name="sourceKeys">The HyperLogLogs keys that will be combined</param>
        /// <returns>Status code</returns>
        public Task<string> PfMerge(string destKey, params string[] sourceKeys)
        {
            return Write(RedisCommand.PfMerge(destKey, sourceKeys));
        }
        #endregion

        private Task<T> Write<T>(RedisCommand<T> command)
        {
            using (new ActivityTracer("Write command async"))
            {
                if (!_connection.Connected)
                    throw new InvalidOperationException("RedisClientAsync is not connected");

                return _connection.CallAsync(command.Parser, command.Command, command.Arguments);
            }
        }

        private void OnAsyncExceptionOccurred(object sender, UnhandledExceptionEventArgs e)
        {
            if (ExceptionOccurred != null)
                ExceptionOccurred(sender, e);
        }

        /// <summary>
        /// Release resources used by the current RedisClientAsync instance
        /// </summary>
        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();

            if (_subscriptionClient.IsValueCreated)
                _subscriptionClient.Value.Dispose();

            if (_activity != null)
                _activity.Dispose();
            _activity = null;
        }
    }
}
