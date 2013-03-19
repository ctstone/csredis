using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
using System.Text;

namespace ctstone.Redis
{
    /// <summary>
    /// Synchronous Redis client 
    /// </summary>
    public class RedisClient : IDisposable
    {
        private static Encoding _encoding = Encoding.UTF8;
        private RedisConnection _connection;
        private RedisPipelineHandler _pipelineHandler;
        private RedisSubscriptionHandler _subscriptionHandler;
        private RedisMonitorHandler _monitorHandler;
        private bool _isTransaction;

        /// <summary>
        /// Get a value indicating that the RedisClient connection is open
        /// </summary>
        public bool Connected { get { return _connection.Connected; } }

        /// <summary>
        /// Get host that the current RedisSentinelClient is connected to
        /// </summary>
        public string Host { get { return _connection.Host; } }

        /// <summary>
        /// Get the port that the current RedisSentinelClient is connected to
        /// </summary>
        public int Port { get { return _connection.Port; } }

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Occurs when a transaction command has been received
        /// </summary>
        public event EventHandler<RedisTransactionQueuedEventArgs> TransactionQueued;

        /// <summary>
        /// Occurs when a monitor response is received
        /// </summary>
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        /// <summary>
        /// Instantiate a new instance of the RedisClient class
        /// </summary>
        /// <param name="host">Redis server host</param>
        /// <param name="port">Redis server port</param>
        /// <param name="timeoutMilliseconds">Connection timeout in milliseconds (0 for no timeout)</param>
        public RedisClient(string host, int port, int timeoutMilliseconds)
        {
            _connection = new RedisConnection(host, port);
            _connection.Connect(timeoutMilliseconds);
            _pipelineHandler = new RedisPipelineHandler(_connection);
            _subscriptionHandler = new RedisSubscriptionHandler(_connection);
            _subscriptionHandler.SubscriptionChanged += OnSubscriptionChanged;
            _subscriptionHandler.SubscriptionReceived += OnSubscriptionReceived;
            _monitorHandler = new RedisMonitorHandler(_connection);
            _monitorHandler.MonitorReceived += OnMonitorReceived;
        }

        /// <summary>
        /// Enter pipeline mode
        /// </summary>
        public void StartPipe()
        {
            _pipelineHandler.Start();
        }

        /// <summary>
        /// Commit pipeline and return results
        /// </summary>
        /// <returns>Array of all pipelined command results</returns>
        public object[] EndPipe()
        {
            return _pipelineHandler.End();
        }

        /// <summary>
        /// Commit pipeline and optionally return results
        /// </summary>
        /// <param name="ignoreResults">Prevent allocation of result array</param>
        /// <returns>Array of all pipelined command results, or null if not returning results</returns>
        public object[] EndPipe(bool ignoreResults)
        {
            return _pipelineHandler.End(ignoreResults);
        }

        /// <summary>
        /// Call arbitrary Redis command (e.g. for a command not yet implemented in this library)
        /// </summary>
        /// <param name="command">The name of the command</param>
        /// <param name="args">Array of arguments to the command</param>
        /// <returns>Redis unified response</returns>
        public object Call(string command, params string[] args)
        {
            return Write(new RedisObject(command, args));
        }

        #region Connection
        /// <summary>
        /// Authenticate to the server
        /// </summary>
        /// <param name="password">Redis server password</param>
        /// <returns>Status message</returns>
        public string Auth(string password)
        {
            return Write(RedisCommand.Auth(password));
        }

        /// <summary>
        /// Echo the given string
        /// </summary>
        /// <param name="message">Message to echo</param>
        /// <returns>Message</returns>
        public string Echo(string message)
        {
            return Write(RedisCommand.Echo(message));
        }

        /// <summary>
        /// Ping the server
        /// </summary>
        /// <returns>Status message</returns>
        public string Ping()
        {
            return Write(RedisCommand.Ping());
        }

        /// <summary>
        /// Close the connection
        /// </summary>
        /// <returns>Status message</returns>
        public string Quit()
        {
            string response = Write(RedisCommand.Quit());
            _connection.Dispose();
            return response;
        }

        /// <summary>
        /// Change the selected database for the current connection
        /// </summary>
        /// <param name="index">Zero-based database index</param>
        /// <returns>Status message</returns>
        public string Select(uint index)
        {
            return Write(RedisCommand.Select(index));
        }
        #endregion

        #region Keys
        /// <summary>
        /// Delete a key
        /// </summary>
        /// <param name="keys">Keys to delete</param>
        /// <returns>Number of keys removed</returns>
        public long Del(params string[] keys)
        {
            return Write(RedisCommand.Del(keys));
        }

        /// <summary>
        /// Return a serialized version of the value stored at the specified key
        /// </summary>
        /// <param name="key">Key to dump</param>
        /// <returns>Serialized value</returns>
        public byte[] Dump(string key)
        {
            return Write(RedisCommand.Dump(key));
        }

        /// <summary>
        /// Determine if a key exists
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>True if key exists</returns>
        public bool Exists(string key)
        {
            return Write(RedisCommand.Exists(key));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest second)</param>
        /// <returns>True if timeout was set; false if key does not exist or timeout could not be set</returns>
        public bool Expire(string key, TimeSpan expiration)
        {
            return Write(RedisCommand.Expire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="seconds">Expiration in seconds</param>
        /// <returns>True if timeout was set; false if key does not exist or timeout could not be set</returns>
        public bool Expire(string key, int seconds)
        {
            return Write(RedisCommand.Expire(key, seconds));
        }

        /// <summary>
        /// Set the expiration for a key (nearest second)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expirationDate">Date of expiration, to nearest second</param>
        /// <returns>True if timeout was set; false if key does not exist or timeout could not be set</returns>
        public bool ExpireAt(string key, DateTime expirationDate)
        {
            return Write(RedisCommand.ExpireAt(key, expirationDate));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp">UNIX timestamp</param>
        /// <returns>True if timeout was set; false if key does not exist or timeout could not be set</returns>
        public bool ExpireAt(string key, int timestamp)
        {
            return Write(RedisCommand.ExpireAt(key, timestamp));
        }

        /// <summary>
        /// Find all keys matching the given pattern
        /// </summary>
        /// <param name="pattern">Pattern to match</param>
        /// <returns>Array of keys matching pattern</returns>
        public string[] Keys(string pattern)
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
        /// <param name="timeoutMilliseconds">Timeout in milliseconds</param>
        /// <returns>Status message</returns>
        public string Migrate(string host, int port, string key, int destinationDb, int timeoutMilliseconds)
        {
            return Write(RedisCommand.Migrate(host, port, key, destinationDb, timeoutMilliseconds));
        }

        /// <summary>
        /// Atomically transfer a key from a Redis instance to another one
        /// </summary>
        /// <param name="host">Remote Redis host</param>
        /// <param name="port">Remote Redis port</param>
        /// <param name="key">Key to migrate</param>
        /// <param name="destinationDb">Remote database ID</param>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds</param>
        /// <returns>Status message</returns>
        public string Migrate(string host, int port, string key, int destinationDb, TimeSpan timeout)
        {
            return Write(RedisCommand.Migrate(host, port, key, destinationDb, timeout));
        }

        /// <summary>
        /// Move a key to another database
        /// </summary>
        /// <param name="key">Key to move</param>
        /// <param name="database">Database destination ID</param>
        /// <returns>True if key was moved</returns>
        public bool Move(string key, int database)
        {
            return Write(RedisCommand.Move(key, database));
        }

        /// <summary>
        /// Get the number of references of the value associated with the specified key
        /// </summary>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>The type of internal representation used to store the value at the specified key</returns>
        public string ObjectEncoding(params string[] arguments)
        {
            return Write(RedisCommand.ObjectEncoding(arguments));
        }

        /// <summary>
        /// Inspect the internals of Redis objects
        /// </summary>
        /// <param name="subCommand">Type of Object command to send</param>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>Varies depending on subCommand</returns>
        public long? Object(RedisObjectSubCommand subCommand, params string[] arguments)
        {
            return Write(RedisCommand.Object(subCommand, arguments));
        }

        /// <summary>
        /// Remove the expiration from a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>True if timeout was removed</returns>
        public bool Persist(string key)
        {
            return Write(RedisCommand.Persist(key));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest millisecond)</param>
        /// <returns>True if timeout was set</returns>
        public bool PExpire(string key, TimeSpan expiration)
        {
            return Write(RedisCommand.PExpire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="milliseconds">Expiration in milliseconds</param>
        /// <returns>True if timeout was set</returns>
        public bool PExpire(string key, long milliseconds)
        {
            return Write(RedisCommand.PExpire(key, milliseconds));
        }

        /// <summary>
        /// Set the expiration for a key (nearest millisecond)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="date">Expiration date</param>
        /// <returns>True if timeout was set</returns>
        public bool PExpireAt(string key, DateTime date)
        {
            return Write(RedisCommand.PExpireAt(key, date));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp specified in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp">Expiration timestamp (milliseconds)</param>
        /// <returns>True if timeout was set</returns>
        public bool PExpireAt(string key, long timestamp)
        {
            return Write(RedisCommand.PExpireAt(key, timestamp));
        }

        /// <summary>
        /// Get the time to live for a key in milliseconds
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>Time-to-live in milliseconds</returns>
        public long PTtl(string key)
        {
            return Write(RedisCommand.PTtl(key));
        }

        /// <summary>
        /// Return a random key from the keyspace
        /// </summary>
        /// <returns>A random key</returns>
        public string RandomKey()
        {
            return Write(RedisCommand.RandomKey());
        }

        /// <summary>
        /// Rename a key
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns>Status code</returns>
        public string Rename(string key, string newKey)
        {
            return Write(RedisCommand.Rename(key, newKey));
        }

        /// <summary>
        /// Rename a key, only if the new key does not exist
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns>True if key was renamed</returns>
        public bool RenameNx(string key, string newKey)
        {
            return Write(RedisCommand.RenameNx(key, newKey));
        }

        /// <summary>
        /// Create a key using the provided serialized value, previously obtained using dump
        /// </summary>
        /// <param name="key">Key to restore</param>
        /// <param name="ttl">Time-to-live in milliseconds</param>
        /// <param name="serializedValue">Serialized value from DUMP</param>
        /// <returns>Status code</returns>
        public string Restore(string key, long ttl, string serializedValue)
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
        /// <returns>The sorted list</returns>
        public string[] Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
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
        /// <returns>Number of elements stored</returns>
        public long SortAndStore(string key, string destination, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = false, params string[] get)
        {
            return Write(RedisCommand.SortAndStore(key, destination, offset, count, by, dir, isAlpha, get));
        }

        /// <summary>
        /// Get the time to live for a key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>Time-to-live in seconds</returns>
        public long Ttl(string key)
        {
            return Write(RedisCommand.Ttl(key));
        }

        /// <summary>
        /// Determine the type stored at key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>Type of key</returns>
        public string Type(string key)
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
        public long HDel(string key, params string[] fields)
        {
            return Write(RedisCommand.HDel(key, fields));
        }

        /// <summary>
        /// Determine if a hash field exists
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to check</param>
        /// <returns>True if hash field exists</returns>
        public bool HExists(string key, string field)
        {
            return Write(RedisCommand.HExists(key, field));
        }

        /// <summary>
        /// Get the value of a hash field
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to get</param>
        /// <returns>Value of hash field</returns>
        public string HGet(string key, string field)
        {
            return Write(RedisCommand.HGet(key, field));
        }

        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <typeparam name="T">Object to map hash</typeparam>
        /// <param name="key">Hash key</param>
        /// <returns>Strongly typed object mapped from hash</returns>
        public T HGetAll<T>(string key)
            where T : new()
        {
            return Write(RedisCommand.HGetAll<T>(key));
        }

        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Dictionary mapped from string</returns>
        public Dictionary<string, string> HGetAll(string key)
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
        public long HIncrBy(string key, string field, long increment)
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
        public double HIncrByFloat(string key, string field, double increment)
        {
            return Write(RedisCommand.HIncrByFloat(key, field, increment));
        }

        /// <summary>
        /// Get all the fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>All hash field names</returns>
        public string[] HKeys(string key)
        {
            return Write(RedisCommand.HKeys(key));
        }

        /// <summary>
        /// Get the number of fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Number of fields in hash</returns>
        public long HLen(string key)
        {
            return Write(RedisCommand.HLen(key));
        }

        /// <summary>
        /// Get the values of all the given hash fields
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="fields">Fields to return</param>
        /// <returns>Values of given fields</returns>
        public string[] HMGet(string key, params string[] fields)
        {
            return Write(RedisCommand.HMGet(key, fields));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="dict">Dictionary mapping of hash</param>
        /// <returns>Status code</returns>
        public string HMSet(string key, Dictionary<string, string> dict)
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
        public string HMSet<T>(string key, T obj)
        {
            return Write(RedisCommand.HMSet<T>(key, obj));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="keyValues">Array of [key,value,key,value,..]</param>
        /// <returns>Status code</returns>
        public string HMSet(string key, params string[] keyValues)
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
        public bool HSet(string key, string field, object value)
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
        public bool HSetNx(string key, string field, object value)
        {
            return Write(RedisCommand.HSetNx(key, field, value));
        }

        /// <summary>
        /// Get all the values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Array of all values in hash</returns>
        public string[] HVals(string key)
        {
            return Write(RedisCommand.HVals(key));
        }
        #endregion

        #region Lists
        /// <summary>
        /// Remove and get the first element and key in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List key and list value</returns>
        public Tuple<string, string> BLPopWithKey(int timeout, params string[] keys)
        {
            return Write(RedisCommand.BLPopWithKey(timeout, keys));
        }

        /// <summary>
        /// Remove and get the first element and key in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List key and list value</returns>
        public Tuple<string, string> BLPopWithKey(TimeSpan timeout, params string[] keys)
        {
            return Write(RedisCommand.BLPopWithKey(timeout, keys));
        }

        /// <summary>
        /// Remove and get the first element value in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List value</returns>
        public string BLPop(int timeout, params string[] keys)
        {
            return Write(RedisCommand.BLPop(timeout, keys));
        }

        /// <summary>
        /// Remove and get the first element value in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List value</returns>
        public string BLPop(TimeSpan timeout, params string[] keys)
        {
            return Write(RedisCommand.BLPop(timeout, keys));
        }

        /// <summary>
        /// Remove and get the last element and key in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List key and list value</returns>
        public Tuple<string, string> BRPopWithKey(int timeout, params string[] keys)
        {
            return Write(RedisCommand.BRPopWithKey(timeout, keys));
        }

        /// <summary>
        /// Remove and get the last element and key in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List key and list value</returns>
        public Tuple<string, string> BRPopWithKey(TimeSpan timeout, params string[] keys)
        {
            return Write(RedisCommand.BRPopWithKey(timeout, keys));
        }

        /// <summary>
        /// Remove and get the last element value in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List value</param>
        /// <returns></returns>
        public string BRPop(int timeout, params string[] keys)
        {
            return Write(RedisCommand.BRPop(timeout, keys));
        }

        /// <summary>
        /// Remove and get the last element value in a list, or block until one is available
        /// </summary>
        /// <param name="timeout">Timeout in seconds</param>
        /// <param name="keys">List keys</param>
        /// <returns>List value</returns>
        public string BRPop(TimeSpan timeout, params string[] keys)
        {
            return Write(RedisCommand.BRPop(timeout, keys));
        }

        /// <summary>
        /// Pop a value from a list, push it to another list and return it; or block until one is available
        /// </summary>
        /// <param name="source">Source list key</param>
        /// <param name="destination">Destination key</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <returns>Element popped</returns>
        public string BRPopLPush(string source, string destination, int timeout)
        {
            return Write(RedisCommand.BRPopLPush(source, destination, timeout));
        }

        /// <summary>
        /// Pop a value from a list, push it to another list and return it; or block until one is available
        /// </summary>
        /// <param name="source">Source list key</param>
        /// <param name="destination">Destination key</param>
        /// <param name="timeout">Timeout in seconds</param>
        /// <returns>Element popped</returns>
        public string BRPopLPush(string source, string destination, TimeSpan timeout)
        {
            return Write(RedisCommand.BRPopLPush(source, destination, timeout));
        }

        /// <summary>
        /// Get an element from a list by its index
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="index">Zero-based index of item to return</param>
        /// <returns>Element at index</returns>
        public string LIndex(string key, long index)
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
        public long LInsert(string key, RedisInsert insertType, string pivot, object value)
        {
            return Write(RedisCommand.LInsert(key, insertType, pivot, value));
        }

        /// <summary>
        /// Get the length of a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Length of list at key</returns>
        public long LLen(string key)
        {
            return Write(RedisCommand.LLen(key));
        }

        /// <summary>
        /// Remove and get the first element in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>First element in list</returns>
        public string LPop(string key)
        {
            return Write(RedisCommand.LPop(key));
        }

        /// <summary>
        /// Prepend one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public long LPush(string key, params object[] values)
        {
            return Write(RedisCommand.LPush(key, values));
        }

        /// <summary>
        /// Prepend a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="value">Value to push</param>
        /// <returns>Length of list after push</returns>
        public long LPushX(string key, object value)
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
        public string[] LRange(string key, long start, long stop)
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
        public long LRem(string key, long count, object value)
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
        public string LSet(string key, long index, object value)
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
        public string LTrim(string key, long start, long stop)
        {
            return Write(RedisCommand.LTrim(key, start, stop));
        }

        /// <summary>
        /// Remove and get the last elment in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Value of last list element</returns>
        public string RPop(string key)
        {
            return Write(RedisCommand.RPop(key));
        }

        /// <summary>
        /// Remove the last elment in a list, append it to another list and return it
        /// </summary>
        /// <param name="source">List source key</param>
        /// <param name="destination">Destination key</param>
        /// <returns>Element being popped and pushed</returns>
        public string RPopLPush(string source, string destination)
        {
            return Write(RedisCommand.RPopLPush(source, destination));
        }

        /// <summary>
        /// Append one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public long RPush(string key, params object[] values)
        {
            return Write(RedisCommand.RPush(key, values));
        }

        /// <summary>
        /// Append a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public long RPushX(string key, params object[] values)
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
        public long SAdd(string key, params object[] members)
        {
            return Write(RedisCommand.SAdd(key, members));
        }

        /// <summary>
        /// Get the number of members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>Number of elements in set</returns>
        public long SCard(string key)
        {
            return Write(RedisCommand.SCard(key));
        }

        /// <summary>
        /// Subtract multiple sets
        /// </summary>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Array of elements in resulting set</returns>
        public string[] SDiff(params string[] keys)
        {
            return Write(RedisCommand.SDiff(keys));
        }

        /// <summary>
        /// Subtract multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Number of elements in the resulting set</returns>
        public long SDiffStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SDiffStore(destination, keys));
        }

        /// <summary>
        /// Intersect multiple sets
        /// </summary>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Array of elements in resulting set</returns>
        public string[] SInter(params string[] keys)
        {
            return Write(RedisCommand.SInter(keys));
        }

        /// <summary>
        /// Intersect multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Number of elements in resulting set</returns>
        public long SInterStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SInterStore(destination, keys));
        }

        /// <summary>
        /// Determine if a given value is a member of a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>True if member exists in set</returns>
        public bool SIsMember(string key, object member)
        {
            return Write(RedisCommand.SIsMember(key, member));
        }

        /// <summary>
        /// Get all the members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>All elements in the set</returns>
        public string[] SMembers(string key)
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
        public bool SMove(string source, string destination, object member)
        {
            return Write(RedisCommand.SMove(source, destination, member));
        }

        /// <summary>
        /// Remove and return a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>The removed element</returns>
        public string SPop(string key)
        {
            return Write(RedisCommand.SPop(key));
        }

        /// <summary>
        /// Get a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>One random element from set</returns>
        public string SRandMember(string key)
        {
            return Write(RedisCommand.SRandMember(key));
        }

        /// <summary>
        /// Get one or more random members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>One or more random elements from set</returns>
        public string[] SRandMember(string key, long count)
        {
            return Write(RedisCommand.SRandMember(key, count));
        }

        /// <summary>
        /// Remove one or more members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="members">Set members to remove</param>
        /// <returns>Number of elements removed from set</returns>
        public long SRem(string key, params object[] members)
        {
            return Write(RedisCommand.SRem(key, members));
        }

        /// <summary>
        /// Add multiple sets
        /// </summary>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Array of elements in resulting set</returns>
        public string[] SUnion(params string[] keys)
        {
            return Write(RedisCommand.SUnion(keys));
        }

        /// <summary>
        /// Add multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Number of elements in resulting set</returns>
        public long SUnionStore(string destination, params string[] keys)
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
        public long ZAdd(string key, params Tuple<double, string>[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Add one or more members to a sorted set, or update its score if it already exists
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="memberScores">Array of member scores [s1, m1, s2, m2, ..]</param>
        /// <returns>Number of elements added to the sorted set (not including member updates)</returns>
        public long ZAdd(string key, params string[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Get the number of members in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <returns>Number of elements in the sorted set</returns>
        public long ZCard(string key)
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
        public long ZCount(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
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
        public double ZIncrBy(string key, double increment, string member)
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
        public long ZInterStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
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
        public string[] ZRange(string key, long start, long stop, bool withScores = false)
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
        public string[] ZRangeByScore(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRangeByScore(key, min, max, withScores, exclusiveMin, exclusiveMax, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member or null if key does not exist</returns>
        public long? ZRank(string key, string member)
        {
            return Write(RedisCommand.ZRank(key, member));
        }

        /// <summary>
        /// Remove one or more members from a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="members">Members to remove</param>
        /// <returns>Number of elements removed</returns>
        public long ZRem(string key, params string[] members)
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
        public long ZRemRangeByRank(string key, long start, long stop)
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
        public long ZRemRangeByScore(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
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
        public string[] ZRevRange(string key, long start, long stop, bool withScores = false)
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
        public string[] ZRevRangeByScore(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRevRangeByScore(key, max, min, withScores, exclusiveMax, exclusiveMin, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member, or null if member does not exist</returns>
        public long? ZRevRank(string key, string member)
        {
            return Write(RedisCommand.ZRevRank(key, member));
        }

        /// <summary>
        /// Get the score associated with the given member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Score of member, or null if member does not exist</returns>
        public double? ZScore(string key, string member)
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
        public long ZUnionStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return Write(RedisCommand.ZUnionStore(destination, weights, aggregate, keys));
        }
        #endregion

        #region Pub/Sub
        /// <summary>
        /// Listen for messages published to channels matching the given patterns
        /// </summary>
        /// <param name="channelPatterns">Patterns to subscribe</param>
        public void PSubscribe(params string[] channelPatterns)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.PSubscribe(channelPatterns));
            //Write(RedisCommand.PSubscribe(channelPatterns));
        }

        /// <summary>
        /// Post a message to a channel
        /// </summary>
        /// <param name="channel">Channel to post message</param>
        /// <param name="message">Message to send</param>
        /// <returns>Number of clients that received the message</returns>
        public long Publish(string channel, string message)
        {
            return Write(RedisCommand.Publish(channel, message));
        }

        /// <summary>
        /// Stop listening for messages posted to channels matching the given patterns
        /// </summary>
        /// <param name="channelPatterns">Patterns to unsubscribe</param>
        public void PUnsubscribe(params string[] channelPatterns)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.PUnsubscribe(channelPatterns));
            //Write(RedisCommand.PUnsubscribe(channelPatterns));
        }

        /// <summary>
        /// Listen for messages published to the given channels
        /// </summary>
        /// <param name="channels">Channels to subscribe</param>
        public void Subscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Subscribe(channels));
            //Write(RedisCommand.Subscribe(channels));
        }

        /// <summary>
        /// Stop listening for messages posted to the given channels
        /// </summary>
        /// <param name="channels">Channels to unsubscribe</param>
        public void Unsubscribe(params string[] channels)
        {
            _subscriptionHandler.HandleSubscription(RedisCommand.Unsubscribe(channels));
            //Write(RedisCommand.Unsubscribe(channels));
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
        public object Eval(string script, string[] keys, params string[] arguments)
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
        public object EvalSHA(string sha1, string[] keys, params string[] arguments)
        {
            return Write(RedisCommand.EvalSHA(sha1, keys, arguments));
        }

        /// <summary>
        /// Check existence of script SHA hashes in the script cache
        /// </summary>
        /// <param name="scripts">SHA1 script hashes</param>
        /// <returns>Array of boolean values indicating script existence on server</returns>
        public bool[] ScriptExists(params string[] scripts)
        {
            return Write(RedisCommand.ScriptExists(scripts));
        }

        /// <summary>
        /// Remove all scripts from the script cache
        /// </summary>
        /// <returns>Status code</returns>
        public string ScriptFlush()
        {
            return Write(RedisCommand.ScriptFlush());
        }

        /// <summary>
        /// Kill the script currently in execution
        /// </summary>
        /// <returns>Status code</returns>
        public string ScriptKill()
        {
            return Write(RedisCommand.ScriptKill());
        }

        /// <summary>
        /// Load the specified Lua script into the script cache
        /// </summary>
        /// <param name="script">Lua script to load</param>
        /// <returns>SHA1 hash of script</returns>
        public string ScriptLoad(string script)
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
        public long Append(string key, object value)
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
        public long BitCount(string key, long? start = null, long? end = null)
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
        public long BitOp(RedisBitOp operation, string destKey, params string[] keys)
        {
            return Write(RedisCommand.BitOp(operation, destKey, keys));
        }

        /// <summary>
        /// Decrement the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after decrement</returns>
        public long Decr(string key)
        {
            return Write(RedisCommand.Decr(key));
        }

        /// <summary>
        /// Decrement the integer value of a key by the given number
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="decrement">Decrement value</param>
        /// <returns>Value of key after decrement</returns>
        public long DecrBy(string key, long decrement)
        {
            return Write(RedisCommand.DecrBy(key, decrement));
        }

        /// <summary>
        /// Get the value of a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Value of key</returns>
        public string Get(string key)
        {
            return Write(RedisCommand.Get(key));
        }

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <param name="offset">Offset of key to check</param>
        /// <returns>Bit value stored at offset</returns>
        public bool GetBit(string key, uint offset)
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
        public string GetRange(string key, long start, long end)
        {
            return Write(RedisCommand.GetRange(key, start, end));
        }

        /// <summary>
        /// Set the string value of a key and return its old value
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Old value stored at key, or null if key did not exist</returns>
        public string GetSet(string key, object value)
        {
            return Write(RedisCommand.GetSet(key, value));
        }

        /// <summary>
        /// Increment the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after increment</returns>
        public long Incr(string key)
        {
            return Write(RedisCommand.Incr(key));
        }

        /// <summary>
        /// Increment the integer value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public long IncrBy(string key, long increment)
        {
            return Write(RedisCommand.IncrBy(key, increment));
        }

        /// <summary>
        /// Increment the float value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public double IncrByFloat(string key, double increment)
        {
            return Write(RedisCommand.IncrByFloat(key, increment));
        }

        /// <summary>
        /// Get the values of all the given keys
        /// </summary>
        /// <param name="keys">Keys to lookup</param>
        /// <returns>Array of values at the specified keys</returns>
        public string[] MGet(params string[] keys)
        {
            return Write(RedisCommand.MGet(keys));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>Status code</returns>
        public string MSet(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>Status code</returns>
        public string MSet(params string[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>True if all keys were set</returns>
        public bool MSetNx(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSetNx(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>True if all keys were set</returns>
        public bool MSetNx(params string[] keyValues)
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
        public string PSetEx(string key, long milliseconds, object value)
        {
            return Write(RedisCommand.PSetEx(key, milliseconds, value));
        }

        /// <summary>
        /// Set the string value of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public string Set(string key, object value)
        {
            return Write(RedisCommand.Set(key, value));
        }

        /// <summary>
        /// Sets or clears the bit at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="offset">Modify key at offset</param>
        /// <param name="value">Value to set (on or off)</param>
        /// <returns>Original bit stored at offset</returns>
        public bool SetBit(string key, uint offset, bool value)
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
        public string SetEx(string key, long seconds, object value)
        {
            return Write(RedisCommand.SetEx(key, seconds, value));
        }

        /// <summary>
        /// Set the value of a key, only if the key does not exist
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if key was set</returns>
        public bool SetNx(string key, object value)
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
        public long SetRange(string key, uint offset, object value)
        {
            return Write(RedisCommand.SetRange(key, offset, value));
        }

        /// <summary>
        /// Get the length of the value stored in a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Length of string at key</returns>
        public long StrLen(string key)
        {
            return Write(RedisCommand.StrLen(key));
        }
        #endregion

        #region Server
        /// <summary>
        /// Asyncronously rewrite the append-only file
        /// </summary>
        /// <returns>Status code</returns>
        public string BgRewriteAof()
        {
            return Write(RedisCommand.BgRewriteAof());
        }

        /// <summary>
        /// Asynchronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public string BgSave()
        {
            return Write(RedisCommand.BgSave());
        }

        /// <summary>
        /// Kill the connection of a client
        /// </summary>
        /// <param name="ip">Client IP returned from CLIENT LIST</param>
        /// <param name="port">Client port returned from CLIENT LIST</param>
        /// <returns>Status code</returns>
        public string ClientKill(string ip, int port)
        {
            return Write(RedisCommand.ClientKill(ip, port));
        }

        /// <summary>
        /// Get the list of client connections
        /// </summary>
        /// <returns>Formatted string of clients</returns>
        public string ClientList()
        {
            return Write(RedisCommand.ClientList());
        }

        /// <summary>
        /// Get the current connection name
        /// </summary>
        /// <returns>Connection name</returns>
        public string ClientGetName()
        {
            return Write(RedisCommand.ClientGetName());
        }

        /// <summary>
        /// Set the current connection name
        /// </summary>
        /// <param name="connectionName">Name of connection (no spaces)</param>
        /// <returns>Status code</returns>
        public string ClientSetName(string connectionName)
        {
            return Write(RedisCommand.ClientSetName(connectionName));
        }

        /// <summary>
        /// Get the value of a configuration paramter
        /// </summary>
        /// <param name="parameter">Configuration parameter to lookup</param>
        /// <returns>Configuration value</returns>
        public string ConfigGet(string parameter)
        {
            return Write(RedisCommand.ConfigGet(parameter));
        }

        /// <summary>
        /// Reset the stats returned by INFO
        /// </summary>
        /// <returns>Status code</returns>
        public string ConfigResetStat()
        {
            return Write(RedisCommand.ConfigResetStat());
        }

        /// <summary>
        /// Set a configuration parameter to the given value
        /// </summary>
        /// <param name="parameter">Parameter to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public string ConfigSet(string parameter, string value)
        {
            return Write(RedisCommand.ConfigSet(parameter, value));
        }

        /// <summary>
        /// Return the number of keys in the selected database
        /// </summary>
        /// <returns>Number of keys</returns>
        public long DbSize()
        {
            return Write(RedisCommand.DbSize());
        }

        /// <summary>
        /// Get debugging information about a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Status code</returns>
        public string DebugObject(string key)
        {
            return Write(RedisCommand.DebugObject(key));
        }

        /// <summary>
        /// Make the server crash :(
        /// </summary>
        /// <returns>Status code</returns>
        public string DebugSegFault()
        {
            return Write(RedisCommand.DebugSegFault());
        }

        /// <summary>
        /// Remove all keys from all databases
        /// </summary>
        /// <returns>Status code</returns>
        public string FlushAll()
        {
            return Write(RedisCommand.FlushAll());
        }

        /// <summary>
        /// Remove all keys from the current database
        /// </summary>
        /// <returns>Status code</returns>
        public string FlushDb()
        {
            return Write(RedisCommand.FlushDb());
        }

        /// <summary>
        /// Get information and statistics about the server
        /// </summary>
        /// <param name="section">all|default|server|clients|memory|persistence|stats|replication|cpu|commandstats|cluster|keyspace</param>
        /// <returns>Formatted string</returns>
        public string Info(string section = null)
        {
            return Write(RedisCommand.Info(section));
        }

        /// <summary>
        /// Get the timestamp of the last successful save to disk
        /// </summary>
        /// <returns>Date of last save</returns>
        public DateTime LastSave()
        {
            return Write(RedisCommand.LastSave());
        }

        /// <summary>
        /// Listen for all requests received by the server in real time
        /// </summary>
        /// <returns>Status code</returns>
        public string Monitor()
        {
            return _monitorHandler.Monitor();
        }

        /// <summary>
        /// Syncronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public string Save()
        {
            return Write(RedisCommand.Save());
        }

        /// <summary>
        /// Syncronously save the dataset to disk an then shut down the server
        /// </summary>
        /// <param name="save">Force a DB saving operation even if no save points are configured</param>
        /// <returns>Status code</returns>
        public string Shutdown(bool? save = null)
        {
            return Write(RedisCommand.Shutdown(save));

        }

        /// <summary>
        /// Make the server a slave of another instance or promote it as master
        /// </summary>
        /// <param name="host">Master host</param>
        /// <param name="port">master port</param>
        /// <returns>Status code</returns>
        public string SlaveOf(string host, int port)
        {
            return Write(RedisCommand.SlaveOf(host, port));
        }

        /// <summary>
        /// Turn off replication, turning the Redis server into a master
        /// </summary>
        /// <returns>Status code</returns>
        public string SlaveOfNoOne()
        {
            return Write(RedisCommand.SlaveOfNoOne());
        }

        /// <summary>
        /// Manges the Redis slow queries log
        /// </summary>
        /// <param name="subCommand">Slowlog sub-command</param>
        /// <param name="argument">Optional argument to sub-command</param>
        /// <returns>Redis unified object</returns>
        public object SlowLog(RedisSlowLog subCommand, string argument = null)
        {
            return Write(RedisCommand.SlowLog(subCommand, argument));
        }

        /// <summary>
        /// Internal command used for replication
        /// </summary>
        /// <returns>Byte array of Redis sync data</returns>
        public byte[] Sync()
        {
            return Write(RedisCommand.Sync());
        }

        /// <summary>
        /// Return the current server time
        /// </summary>
        /// <returns>Server time</returns>
        public DateTime Time()
        {
            return Write(RedisCommand.Time());
        }
        #endregion

        #region Transactions
        /// <summary>
        /// Discard all commands issued after MULTI
        /// </summary>
        /// <returns>Status code</returns>
        public string Discard()
        {
            return Write(RedisCommand.Discard());
        }

        /// <summary>
        /// Execute all commands issued after MULTI
        /// </summary>
        /// <returns>Array of output from all transaction commands</returns>
        public object[] Exec()
        {
            return Write(RedisCommand.Exec());
        }

        /// <summary>
        /// Mark the start of a transaction block
        /// </summary>
        /// <returns>Status code</returns>
        public string Multi()
        {
            return Write(RedisCommand.Multi());
        }

        /// <summary>
        /// Forget about all watched keys
        /// </summary>
        /// <returns>Status code</returns>
        public string Unwatch()
        {
            return Write(RedisCommand.Unwatch());
        }

        /// <summary>
        /// Watch the given keys to determine execution of the MULTI/EXEC block
        /// </summary>
        /// <param name="keys">Keys to watch</param>
        /// <returns>Status code</returns>
        public string Watch(params string[] keys)
        {
            return Write(RedisCommand.Watch(keys));
        }
        #endregion

        
        /// <summary>
        /// Release resources used by the current RedisClient instance
        /// </summary>
        public void Dispose()
        {
            if (_connection != null)
                _connection.Dispose();
        }

        private T Write<T>(RedisCommand<T> command)
        {
            if (!_connection.Connected)
                throw new InvalidOperationException("RedisClient is not connected");

            if (_subscriptionHandler.IsSubscribed)
                throw new InvalidOperationException("RedisClient cannot accept non-subscription commands while subscribed");

            if (_pipelineHandler.Active)
            {
                _pipelineHandler.Write(command.Command, command.Arguments);
                return default(T);
            }
            else if (command.Command == "MULTI")
            {
                _isTransaction = true;
            }
            else if (command.Command == "EXEC" || command.Command == "DISCARD")
            {
                _isTransaction = false;
            }
            else if (_isTransaction)
            {
                string response = _connection.Call(RedisReader.ReadStatus, command.Command, command.Arguments);
                if (TransactionQueued != null)
                    TransactionQueued(this, new RedisTransactionQueuedEventArgs(response));
                return default(T);
            }

            return _connection.Call(command.Parser, command.Command, command.Arguments);
        }

        private void OnSubscriptionReceived(object sender, RedisSubscriptionReceivedEventArgs e)
        {
            if (SubscriptionReceived != null)
                SubscriptionReceived(this, e);
        }

        private void OnSubscriptionChanged(object sender, RedisSubscriptionChangedEventArgs e)
        {
            if (SubscriptionChanged != null)
                SubscriptionChanged(this, e);
        }

        private void OnMonitorReceived(object sender, RedisMonitorEventArgs e)
        {
            if (MonitorReceived != null)
                MonitorReceived(this, e);
        }
    }
}
