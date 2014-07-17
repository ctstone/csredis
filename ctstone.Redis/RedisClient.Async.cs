using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    public partial class RedisClient
    {
        public Task<bool> ConnectAsync()
        {
            return _connection.ConnectAsync();
        }

        public Task<object> CallAsync(string command, params string[] args)
        {
            return WriteAsync(RedisCommand.Call(command, args));
        }

        Task<T> WriteAsync<T>(RedisCommand<T> command)
        {
            if (_transaction.Active)
                return _transaction.WriteAsync(command);
            else
                return _connection.CallAsync(command);
        }

        #region Connection
        /// <summary>
        /// Authenticate to the server
        /// </summary>
        /// <param name="password">Server password</param>
        /// <returns>Task associated with status message</returns>
        public Task<string> AuthAsync(string password)
        {
            _authPassword = password;
            return WriteAsync(RedisCommand.Auth(password));
        }

        /// <summary>
        /// Echo the given string
        /// </summary>
        /// <param name="message">Message to echo</param>
        /// <returns>Task associated with echo response</returns>
        public Task<string> EchoAsync(string message)
        {
            return WriteAsync(RedisCommand.Echo(message));
        }

        /// <summary>
        /// Ping the server
        /// </summary>
        /// <returns>Task associated with status message</returns>
        public Task<string> PingAsync()
        {
            return WriteAsync(RedisCommand.Ping());
        }

        /// <summary>
        /// Close the connection
        /// </summary>
        /// <returns>Task associated with status message</returns>
        public Task<string> QuitAsync()
        {
            return WriteAsync(RedisCommand.Quit())
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
        public Task<long> DelAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.Del(keys));
        }

        /// <summary>
        /// Return a serialized version of the value stored at the specified key
        /// </summary>
        /// <param name="key">Key to dump</param>
        /// <returns></returns>
        public Task<byte[]> DumpAsync(string key)
        {
            return WriteAsync(RedisCommand.Dump(key));
        }

        /// <summary>
        /// Determine if a key exists
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<bool> ExistsAsync(string key)
        {
            return WriteAsync(RedisCommand.Exists(key));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest second)</param>
        /// <returns></returns>
        public Task<bool> ExpireAsync(string key, int expiration)
        {
            return WriteAsync(RedisCommand.Expire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration in seconds</param>
        /// <returns></returns>
        public Task<bool> ExpireAsync(string key, TimeSpan expiration)
        {
            return WriteAsync(RedisCommand.Expire(key, expiration));
        }

        /// <summary>
        /// Set the expiration for a key (nearest second)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expirationDate">Date of expiration, to nearest second</param>
        /// <returns></returns>
        public Task<bool> ExpireAtAsync(string key, DateTime expirationDate)
        {
            return WriteAsync(RedisCommand.ExpireAt(key, expirationDate));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public Task<bool> ExpireAtAsync(string key, int timestamp)
        {
            return WriteAsync(RedisCommand.ExpireAt(key, timestamp));
        }

        /// <summary>
        /// Find all keys matching the given pattern
        /// </summary>
        /// <param name="pattern">Pattern to match</param>
        /// <returns></returns>
        public Task<string[]> KeysAsync(string pattern)
        {
            return WriteAsync(RedisCommand.Keys(pattern));
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
        public Task<string> MigrateAsync(string host, int port, string key, int destinationDb, int timeout)
        {
            return WriteAsync(RedisCommand.Migrate(host, port, key, destinationDb, timeout));
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
        public Task<string> MigrateAsync(string host, int port, string key, int destinationDb, TimeSpan timeout)
        {
            return WriteAsync(RedisCommand.Migrate(host, port, key, destinationDb, timeout));
        }

        /// <summary>
        /// Move a key to another database
        /// </summary>
        /// <param name="key">Key to move</param>
        /// <param name="database">Database destination ID</param>
        /// <returns></returns>
        public Task<bool> MoveAsync(string key, int database)
        {
            return WriteAsync(RedisCommand.Move(key, database));
        }

        /// <summary>
        /// Get the number of references of the value associated with the specified key
        /// </summary>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>The type of internal representation used to store the value at the specified key</returns>
        public Task<string> ObjectEncodingAsync(params string[] arguments)
        {
            return WriteAsync(RedisCommand.ObjectEncoding(arguments));
        }

        /// <summary>
        /// Inspect the internals of Redis objects
        /// </summary>
        /// <param name="subCommand">Type of Object command to send</param>
        /// <param name="arguments">Subcommand arguments</param>
        /// <returns>Varies depending on subCommand</returns>
        public Task<long?> ObjectAsync(RedisObjectSubCommand subCommand, params string[] arguments)
        {
            return WriteAsync(RedisCommand.Object(subCommand, arguments));
        }

        /// <summary>
        /// Remove the expiration from a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns></returns>
        public Task<bool> PersistAsync(string key)
        {
            return WriteAsync(RedisCommand.Persist(key));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="expiration">Expiration (nearest millisecond)</param>
        /// <returns></returns>
        public Task<bool> PExpireAsync(string key, TimeSpan expiration)
        {
            return WriteAsync(RedisCommand.PExpire(key, expiration));
        }

        /// <summary>
        /// Set a key's time to live in milliseconds
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="milliseconds">Expiration in milliseconds</param>
        /// <returns></returns>
        public Task<bool> PExpireAsync(string key, long milliseconds)
        {
            return WriteAsync(RedisCommand.PExpire(key, milliseconds));
        }

        /// <summary>
        /// Set the expiration for a key (nearest millisecond)
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="date">Expiration date</param>
        /// <returns></returns>
        public Task<bool> PExpireAtAsync(string key, DateTime date)
        {
            return WriteAsync(RedisCommand.PExpireAt(key, date));
        }

        /// <summary>
        /// Set the expiration for a key as a UNIX timestamp specified in milliseconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="timestamp">Expiration timestamp (milliseconds)</param>
        /// <returns></returns>
        public Task<bool> PExpireAtAsync(string key, long timestamp)
        {
            return WriteAsync(RedisCommand.PExpireAt(key, timestamp));
        }

        /// <summary>
        /// Get the time to live for a key in milliseconds
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<long> PTtlAsync(string key)
        {
            return WriteAsync(RedisCommand.PTtl(key));
        }

        /// <summary>
        /// Return a random key from the keyspace
        /// </summary>
        /// <returns></returns>
        public Task<string> RandomKeyAsync()
        {
            return WriteAsync(RedisCommand.RandomKey());
        }

        /// <summary>
        /// Rename a key
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns></returns>
        public Task<string> RenameAsync(string key, string newKey)
        {
            return WriteAsync(RedisCommand.Rename(key, newKey));
        }

        /// <summary>
        /// Rename a key, only if the new key does not exist
        /// </summary>
        /// <param name="key">Key to rename</param>
        /// <param name="newKey">New key name</param>
        /// <returns></returns>
        public Task<bool> RenameNxAsync(string key, string newKey)
        {
            return WriteAsync(RedisCommand.RenameNx(key, newKey));
        }

        /// <summary>
        /// Create a key using the provided serialized value, previously obtained using dump
        /// </summary>
        /// <param name="key">Key to restore</param>
        /// <param name="ttl">Time-to-live in milliseconds</param>
        /// <param name="serializedValue">Serialized value from DUMP</param>
        /// <returns></returns>
        public Task<string> RestoreAsync(string key, long ttl, string serializedValue)
        {
            return WriteAsync(RedisCommand.Restore(key, ttl, serializedValue));
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
        public Task<string[]> SortAsync(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            return WriteAsync(RedisCommand.Sort(key, offset, count, by, dir, isAlpha, get));
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
        public Task<long> SortAndStoreAsync(string key, string destination, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            return WriteAsync(RedisCommand.SortAndStore(key, destination, offset, count, by, dir, isAlpha, get));
        }

        /// <summary>
        /// Get the time to live for a key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<long> TtlAsync(string key)
        {
            return WriteAsync(RedisCommand.Ttl(key));
        }

        /// <summary>
        /// Determine the type stored at key
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns></returns>
        public Task<string> TypeAsync(string key)
        {
            return WriteAsync(RedisCommand.Type(key));
        }
        #endregion

        #region Hashes
        /// <summary>
        /// Delete one or more hash fields
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="fields">Fields to delete</param>
        /// <returns>Number of fields removed from hash</returns>
        public Task<long> HDelAsync(string key, params string[] fields)
        {
            return WriteAsync(RedisCommand.HDel(key, fields));
        }

        /// <summary>
        /// Determine if a hash field exists
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to check</param>
        /// <returns>True if hash field exists</returns>
        public Task<bool> HExistsAsync(string key, string field)
        {
            return WriteAsync(RedisCommand.HExists(key, field));
        }

        /// <summary>
        /// Get the value of a hash field
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to get</param>
        /// <returns>Value of hash field</returns>
        public Task<string> HGetAsync(string key, string field)
        {
            return WriteAsync(RedisCommand.HGet(key, field));
        }

        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <typeparam name="T">Object to map hash</typeparam>
        /// <param name="key">Hash key</param>
        /// <returns>Strongly typed object mapped from hash</returns>
        public Task<T> HGetAllAsync<T>(string key)
            where T : class
        {
            return WriteAsync(RedisCommand.HGetAll<T>(key));
        }

        /// <summary>
        /// Get all the fields and values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Dictionary mapped from string</returns>
        public Task<Dictionary<string, string>> HGetAllAsync(string key)
        {
            return WriteAsync(RedisCommand.HGetAll(key));
        }

        /// <summary>
        /// Increment the integer value of a hash field by the given number
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to increment</param>
        /// <param name="increment">Increment value</param>
        /// <returns>Value of field after increment</returns>
        public Task<long> HIncrByAsync(string key, string field, long increment)
        {
            return WriteAsync(RedisCommand.HIncrBy(key, field, increment));
        }

        /// <summary>
        /// Increment the float value of a hash field by the given number
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Field to increment</param>
        /// <param name="increment">Increment value</param>
        /// <returns>Value of field after increment</returns>
        public Task<double> HIncrByFloatAsync(string key, string field, double increment)
        {
            return WriteAsync(RedisCommand.HIncrByFloat(key, field, increment));
        }

        /// <summary>
        /// Get all the fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>All hash field names</returns>
        public Task<string[]> HKeysAsync(string key)
        {
            return WriteAsync(RedisCommand.HKeys(key));
        }

        /// <summary>
        /// Get the number of fields in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Number of fields in hash</returns>
        public Task<long> HLenAsync(string key)
        {
            return WriteAsync(RedisCommand.HLen(key));
        }

        /// <summary>
        /// Get the values of all the given hash fields
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="fields">Fields to return</param>
        /// <returns>Values of given fields</returns>
        public Task<string[]> HMGetAsync(string key, params string[] fields)
        {
            return WriteAsync(RedisCommand.HMGet(key, fields));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="dict">Dictionary mapping of hash</param>
        /// <returns>Status code</returns>
        public Task<string> HMSetAsync(string key, Dictionary<string, string> dict)
        {
            return WriteAsync(RedisCommand.HMSet(key, dict));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <typeparam name="T">Type of object to map hash</typeparam>
        /// <param name="key">Hash key</param>
        /// <param name="obj">Object mapping of hash</param>
        /// <returns>Status code</returns>
        public Task<string> HMSetAsync<T>(string key, T obj)
            where T : class
        {
            return WriteAsync(RedisCommand.HMSet<T>(key, obj));
        }

        /// <summary>
        /// Set multiple hash fields to multiple values
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="keyValues">Array of [key,value,key,value,..]</param>
        /// <returns>Status code</returns>
        public Task<string> HMSetAsync(string key, params string[] keyValues)
        {
            return WriteAsync(RedisCommand.HMSet(key, keyValues));
        }

        /// <summary>
        /// Set the value of a hash field
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Hash field to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if field is new</returns>
        public Task<bool> HSetAsync(string key, string field, object value)
        {
            return WriteAsync(RedisCommand.HSet(key, field, value));
        }

        /// <summary>
        /// Set the value of a hash field, only if the field does not exist
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <param name="field">Hash field to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if field was set to value</returns>
        public Task<bool> HSetNxAsync(string key, string field, object value)
        {
            return WriteAsync(RedisCommand.HSetNx(key, field, value));
        }

        /// <summary>
        /// Get all the values in a hash
        /// </summary>
        /// <param name="key">Hash key</param>
        /// <returns>Array of all values in hash</returns>
        public Task<string[]> HValsAsync(string key)
        {
            return WriteAsync(RedisCommand.HVals(key));
        }
        #endregion

        #region Lists
        /// <summary>
        /// Get an element from a list by its index
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="index">Zero-based index of item to return</param>
        /// <returns>Element at index</returns>
        public Task<string> LIndexAsync(string key, long index)
        {
            return WriteAsync(RedisCommand.LIndex(key, index));
        }

        /// <summary>
        /// Insert an element before or after another element in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="insertType">Relative position</param>
        /// <param name="pivot">Relative element</param>
        /// <param name="value">Element to insert</param>
        /// <returns>Length of list after insert or -1 if pivot not found</returns>
        public Task<long> LInsertAsync(string key, RedisInsert insertType, string pivot, object value)
        {
            return WriteAsync(RedisCommand.LInsert(key, insertType, pivot, value));
        }

        /// <summary>
        /// Get the length of a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Length of list at key</returns>
        public Task<long> LLenAsync(string key)
        {
            return WriteAsync(RedisCommand.LLen(key));
        }

        /// <summary>
        /// Remove and get the first element in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>First element in list</returns>
        public Task<string> LPopAsync(string key)
        {
            return WriteAsync(RedisCommand.LPop(key));
        }

        /// <summary>
        /// Prepend one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> LPushAsync(string key, params object[] values)
        {
            return WriteAsync(RedisCommand.LPush(key, values));
        }

        /// <summary>
        /// Prepend a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="value">Value to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> LPushXAsync(string key, object value)
        {
            return WriteAsync(RedisCommand.LPushX(key, value));
        }

        /// <summary>
        /// Get a range of elements from a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <returns>List of elements in range</returns>
        public Task<string[]> LRangeAsync(string key, long start, long stop)
        {
            return WriteAsync(RedisCommand.LRange(key, start, stop));
        }

        /// <summary>
        /// Remove elements from a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="count">&gt;0: remove N elements from head to tail; &lt;0: remove N elements from tail to head; =0: remove all elements</param>
        /// <param name="value">Remove elements equal to value</param>
        /// <returns>Number of removed elements</returns>
        public Task<long> LRemAsync(string key, long count, object value)
        {
            return WriteAsync(RedisCommand.LRem(key, count, value));
        }

        /// <summary>
        /// Set the value of an element in a list by its index
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="index">List index to modify</param>
        /// <param name="value">New element value</param>
        /// <returns>Status code</returns>
        public Task<string> LSetAsync(string key, long index, object value)
        {
            return WriteAsync(RedisCommand.LSet(key, index, value));
        }

        /// <summary>
        /// Trim a list to the specified range
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="start">Zero-based start index</param>
        /// <param name="stop">Zero-based stop index</param>
        /// <returns>Status code</returns>
        public Task<string> LTrimAsync(string key, long start, long stop)
        {
            return WriteAsync(RedisCommand.LTrim(key, start, stop));
        }

        /// <summary>
        /// Remove and get the last elment in a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <returns>Value of last list element</returns>
        public Task<string> RPopAsync(string key)
        {
            return WriteAsync(RedisCommand.RPop(key));
        }

        /// <summary>
        /// Remove the last elment in a list, append it to another list and return it
        /// </summary>
        /// <param name="source">List source key</param>
        /// <param name="destination">Destination key</param>
        /// <returns>Element being popped and pushed</returns>
        public Task<string> RPopLPushAsync(string source, string destination)
        {
            return WriteAsync(RedisCommand.RPopLPush(source, destination));
        }

        /// <summary>
        /// Append one or multiple values to a list
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> RPushAsync(string key, params object[] values)
        {
            return WriteAsync(RedisCommand.RPush(key, values));
        }

        /// <summary>
        /// Append a value to a list, only if the list exists
        /// </summary>
        /// <param name="key">List key</param>
        /// <param name="values">Values to push</param>
        /// <returns>Length of list after push</returns>
        public Task<long> RPushXAsync(string key, params object[] values)
        {
            return WriteAsync(RedisCommand.RPushX(key, values));
        }
        #endregion

        #region Sets
        /// <summary>
        /// Add one or more members to a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="members">Members to add to set</param>
        /// <returns>Number of elements added to set</returns>
        public Task<long> SAddAsync(string key, params object[] members)
        {
            return WriteAsync(RedisCommand.SAdd(key, members));
        }

        /// <summary>
        /// Get the number of members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>Number of elements in set</returns>
        public Task<long> SCardAsync(string key)
        {
            return WriteAsync(RedisCommand.SCard(key));
        }

        /// <summary>
        /// Subtract multiple sets
        /// </summary>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SDiffAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.SDiff(keys));
        }

        /// <summary>
        /// Subtract multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to subtract</param>
        /// <returns>Number of elements in the resulting set</returns>
        public Task<long> SDiffStoreAsync(string destination, params string[] keys)
        {
            return WriteAsync(RedisCommand.SDiffStore(destination, keys));
        }

        /// <summary>
        /// Intersect multiple sets
        /// </summary>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SInterAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.SInter(keys));
        }

        /// <summary>
        /// Intersect multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to intersect</param>
        /// <returns>Number of elements in resulting set</returns>
        public Task<long> SInterStoreAsync(string destination, params string[] keys)
        {
            return WriteAsync(RedisCommand.SInterStore(destination, keys));
        }

        /// <summary>
        /// Determine if a given value is a member of a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>True if member exists in set</returns>
        public Task<bool> SIsMemberAsync(string key, object member)
        {
            return WriteAsync(RedisCommand.SIsMember(key, member));
        }

        /// <summary>
        /// Get all the members in a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>All elements in the set</returns>
        public Task<string[]> SMembersAsync(string key)
        {
            return WriteAsync(RedisCommand.SMembers(key));
        }

        /// <summary>
        /// Move a member from one set to another
        /// </summary>
        /// <param name="source">Source key</param>
        /// <param name="destination">Destination key</param>
        /// <param name="member">Member to move</param>
        /// <returns>True if element was moved</returns>
        public Task<bool> SMoveAsync(string source, string destination, object member)
        {
            return WriteAsync(RedisCommand.SMove(source, destination, member));
        }

        /// <summary>
        /// Remove and return a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>The removed element</returns>
        public Task<string> SPopAsync(string key)
        {
            return WriteAsync(RedisCommand.SPop(key));
        }

        /// <summary>
        /// Get a random member from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <returns>One random element from set</returns>
        public Task<string> SRandMemberAsync(string key)
        {
            return WriteAsync(RedisCommand.SRandMember(key));
        }

        /// <summary>
        /// Get one or more random members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>One or more random elements from set</returns>
        public Task<string[]> SRandMemberAsync(string key, long count)
        {
            return WriteAsync(RedisCommand.SRandMember(key, count));
        }

        /// <summary>
        /// Remove one or more members from a set
        /// </summary>
        /// <param name="key">Set key</param>
        /// <param name="members">Set members to remove</param>
        /// <returns>Number of elements removed from set</returns>
        public Task<long> SRemAsync(string key, params object[] members)
        {
            return WriteAsync(RedisCommand.SRem(key, members));
        }

        /// <summary>
        /// Add multiple sets
        /// </summary>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Array of elements in resulting set</returns>
        public Task<string[]> SUnionAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.SUnion(keys));
        }

        /// <summary>
        /// Add multiple sets and store the resulting set in a key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Set keys to union</param>
        /// <returns>Number of elements in resulting set</returns>
        public Task<long> SUnionStoreAsync(string destination, params string[] keys)
        {
            return WriteAsync(RedisCommand.SUnionStore(destination, keys));
        }
        #endregion

        #region Sorted Sets
        /// <summary>
        /// Add one or more members to a sorted set, or update its score if it already exists
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="memberScores">Array of member scores to add to sorted set</param>
        /// <returns>Number of elements added to the sorted set (not including member updates)</returns>
        public Task<long> ZAddAsync<TScore, TMember>(string key, params Tuple<TScore, TMember>[] memberScores)
        {
            return WriteAsync(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Add one or more members to a sorted set, or update its score if it already exists
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="memberScores">Array of member scores [s1, m1, s2, m2, ..]</param>
        /// <returns>Number of elements added to the sorted set (not including member updates)</returns>
        public Task<long> ZAddAsync(string key, params string[] memberScores)
        {
            return WriteAsync(RedisCommand.ZAdd(key, memberScores));
        }

        /// <summary>
        /// Get the number of members in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <returns>Number of elements in the sorted set</returns>
        public Task<long> ZCardAsync(string key)
        {
            return WriteAsync(RedisCommand.ZCard(key));
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
        public Task<long> ZCountAsync(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return WriteAsync(RedisCommand.ZCount(key, min, max, exclusiveMin, exclusiveMax));
        }

        /// <summary>
        /// Count the members in a sorted set with scores within the given values
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <returns>Number of elements in the specified score range</returns>
        public Task<long> ZCountAsync(string key, string min, string max)
        {
            return WriteAsync(RedisCommand.ZCount(key, min, max));
        }

        /// <summary>
        /// Increment the score of a member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="increment">Increment by value</param>
        /// <param name="member">Sorted set member to increment</param>
        /// <returns>New score of member</returns>
        public Task<double> ZIncrByAsync(string key, double increment, string member)
        {
            return WriteAsync(RedisCommand.ZIncrBy(key, increment, member));
        }

        /// <summary>
        /// Intersect multiple sorted sets and store the resulting set in a new key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="weights">Multiplication factor for each input set</param>
        /// <param name="aggregate">Aggregation function of resulting set</param>
        /// <param name="keys">Sorted set keys to intersect</param>
        /// <returns>Number of elements in the resulting sorted set</returns>
        public Task<long> ZInterStoreAsync(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return WriteAsync(RedisCommand.ZInterStore(destination, weights, aggregate, keys));
        }

        /// <summary>
        /// Intersect multiple sorted sets and store the resulting set in a new key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="keys">Sorted set keys to intersect</param>
        /// <returns>Number of elements in the resulting sorted set</returns>
        public Task<long> ZInterStoreAsync(string destination, params string[] keys)
        {
            return ZInterStoreAsync(destination, null, null, keys);
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <param name="withScores">Include scores in result</param>
        /// <returns>Array of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRangeAsync(string key, long start, long stop, bool withScores = false)
        {
            return WriteAsync(RedisCommand.ZRange(key, start, stop, withScores));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index, with scores
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <returns>Array of elements in the specified range with scores</returns>
        public Task<Tuple<string, double>[]> ZRangeWithScoresAsync(string key, long start, long stop)
        {
            return WriteAsync(RedisCommand.ZRangeWithScores(key, start, stop));
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
        public Task<string[]> ZRangeByScoreAsync(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRangeByScore(key, min, max, withScores, exclusiveMin, exclusiveMax, offset, count));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="withScores">Include scores in result</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRangeByScoreAsync(string key, string min, string max, bool withScores = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRangeByScore(key, min, max, withScores, offset, count));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score, with scores
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="exclusiveMin">Minimum score is exclusive</param>
        /// <param name="exclusiveMax">Maximum score is exclusive</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<Tuple<string, double>[]> ZRangeByScoreWithScoresAsync(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRangeByScoreWithScores(key, min, max, exclusiveMin, exclusiveMax, offset, count));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score, with scores
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Minimum score</param>
        /// <param name="max">Maximum score</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<Tuple<string, double>[]> ZRangeByScoreWithScoresAsync(string key, string min, string max, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRangeByScoreWithScores(key, min, max, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member or null if key does not exist</returns>
        public Task<long?> ZRankAsync(string key, string member)
        {
            return WriteAsync(RedisCommand.ZRank(key, member));
        }

        /// <summary>
        /// Remove one or more members from a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="members">Members to remove</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemAsync(string key, params object[] members)
        {
            return WriteAsync(RedisCommand.ZRem(key, members));
        }

        /// <summary>
        /// Remove all members in a sorted set within the given indexes
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemRangeByRankAsync(string key, long start, long stop)
        {
            return WriteAsync(RedisCommand.ZRemRangeByRank(key, start, stop));
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
        public Task<long> ZRemRangeByScoreAsync(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return WriteAsync(RedisCommand.ZRemRangeByScore(key, min, max, exclusiveMin, exclusiveMax));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <param name="withScores">Include scores in result</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<string[]> ZRevRangeAsync(string key, long start, long stop, bool withScores = false)
        {
            return WriteAsync(RedisCommand.ZRevRange(key, start, stop, withScores));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by index, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="start">Start offset</param>
        /// <param name="stop">Stop offset</param>
        /// <param name="withScores">Include scores in result</param>
        /// <returns>List of elements in the specified range (with optional scores)</returns>
        public Task<Tuple<string, double>[]> ZRevRangeWithScoresAsync(string key, long start, long stop)
        {
            return WriteAsync(RedisCommand.ZRevRangeWithScores(key, start, stop));
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
        public Task<string[]> ZRevRangeByScoreAsync(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRevRangeByScore(key, max, min, withScores, exclusiveMax, exclusiveMin, offset, count));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="max">Maximum score</param>
        /// <param name="min">Minimum score</param>
        /// <param name="withScores">Include scores in result</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified score range (with optional scores)</returns>
        public Task<string[]> ZRevRangeByScoreAsync(string key, string max, string min, bool withScores = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRevRangeByScore(key, max, min, withScores, offset, count));
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
        public Task<Tuple<string, double>[]> ZRevRangeByScoreWithScoresAsync(string key, double max, double min, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRevRangeByScoreWithScores(key, max, min, exclusiveMax, exclusiveMin, offset, count));
        }

        /// <summary>
        /// Return a range of members in a sorted set, by score, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="max">Maximum score</param>
        /// <param name="min">Minimum score</param>
        /// <param name="withScores">Include scores in result</param>
        /// <param name="offset">Start offset</param>
        /// <param name="count">Number of elements to return</param>
        /// <returns>List of elements in the specified score range (with optional scores)</returns>
        public Task<Tuple<string, double>[]> ZRevRangeByScoreWithScoresAsync(string key, string max, string min, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRevRangeByScoreWithScores(key, max, min, offset, count));
        }

        /// <summary>
        /// Determine the index of a member in a sorted set, with scores ordered from high to low
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Rank of member, or null if member does not exist</returns>
        public Task<long?> ZRevRankAsync(string key, string member)
        {
            return WriteAsync(RedisCommand.ZRevRank(key, member));
        }

        /// <summary>
        /// Get the score associated with the given member in a sorted set
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="member">Member to lookup</param>
        /// <returns>Score of member, or null if member does not exist</returns>
        public Task<double?> ZScoreAsync(string key, string member)
        {
            return WriteAsync(RedisCommand.ZScore(key, member));
        }

        /// <summary>
        /// Add multiple sorted sets and store the resulting sorted set in a new key
        /// </summary>
        /// <param name="destination">Destination key</param>
        /// <param name="weights">Multiplication factor for each input set</param>
        /// <param name="aggregate">Aggregation function of resulting set</param>
        /// <param name="keys">Sorted set keys to union</param>
        /// <returns>Number of elements in the resulting sorted set</returns>
        public Task<long> ZUnionStoreAsync(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return WriteAsync(RedisCommand.ZUnionStore(destination, weights, aggregate, keys));
        }

        /// <summary>
        /// Iterate the scores and elements of a sorted set field
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="cursor">The cursor returned by the server in the previous call, or 0 if this is the first call</param>
        /// <param name="pattern">Glob-style pattern to filter returned elements</param>
        /// <param name="count">Maximum number of elements to return</param>
        /// <returns>Updated cursor and result set</returns>
        public Task<RedisScan<Tuple<string, double>>> ZScanAsync(string key, long cursor, string pattern = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZScan(key, cursor, pattern, count));
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
        public Task<string[]> ZRangeByLexAsync(string key, string min, string max, long? offset = null, long? count = null)
        {
            return WriteAsync(RedisCommand.ZRangeByLex(key, min, max, offset, count));
        }

        /// <summary>
        /// Remove all elements in the sorted set with a value between min and max
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Lexagraphic start value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="max">Lexagraphic stop value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <returns>Number of elements removed</returns>
        public Task<long> ZRemRangeByLexAsync(string key, string min, string max)
        {
            return WriteAsync(RedisCommand.ZRemRangeByLex(key, min, max));
        }

        /// <summary>
        /// Returns the number of elements in the sorted set with a value between min and max.
        /// </summary>
        /// <param name="key">Sorted set key</param>
        /// <param name="min">Lexagraphic start value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <param name="max">Lexagraphic stop value. Prefix value with '(' to indicate exclusive; '[' to indicate inclusive. Use '-' or '+' to specify infinity.</param>
        /// <returns>Number of elements in the specified score range</returns>
        public Task<long> ZLexCountAsync(string key, string min, string max)
        {
            return WriteAsync(RedisCommand.ZLexCount(key, min, max));
        }
        #endregion

        #region Pub/Sub
        /// <summary>
        /// Post a message to a channel
        /// </summary>
        /// <param name="channel">Channel to post message</param>
        /// <param name="message">Message to send</param>
        /// <returns>Number of clients that received the message</returns>
        public Task<long> PublishAsync(string channel, string message)
        {
            return WriteAsync(RedisCommand.Publish(channel, message));
        }

        public Task<string[]> PubSubChannelsAsync(string pattern)
        {
            return WriteAsync(RedisCommand.PubSubChannels(pattern));
        }

        public Task<Tuple<string, long>[]> PubSubNumSubAsync(params string[] channels)
        {
            return WriteAsync(RedisCommand.PubSubNumSub(channels));
        }

        public Task<long> PubSubNumPatAsync()
        {
            return WriteAsync(RedisCommand.PubSubNumPat());
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
        public Task<object> EvalAsync(string script, string[] keys, params string[] arguments)
        {
            return WriteAsync(RedisCommand.Eval(script, keys, arguments));
        }

        /// <summary>
        /// Execute a Lua script server side, sending only the script's cached SHA hash
        /// </summary>
        /// <param name="sha1">SHA1 hash of script</param>
        /// <param name="keys">Keys used by script</param>
        /// <param name="arguments">Arguments to pass to script</param>
        /// <returns>Redis object</returns>
        public Task<object> EvalSHAAsync(string sha1, string[] keys, params string[] arguments)
        {
            return WriteAsync(RedisCommand.EvalSHA(sha1, keys, arguments));
        }

        /// <summary>
        /// Check existence of script SHA hashes in the script cache
        /// </summary>
        /// <param name="scripts">SHA1 script hashes</param>
        /// <returns>Array of boolean values indicating script existence on server</returns>
        public Task<bool[]> ScriptExistsAsync(params string[] scripts)
        {
            return WriteAsync(RedisCommand.ScriptExists(scripts));
        }

        /// <summary>
        /// Remove all scripts from the script cache
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ScriptFlushAsync()
        {
            return WriteAsync(RedisCommand.ScriptFlush());
        }

        /// <summary>
        /// Kill the script currently in execution
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ScriptKillAsync()
        {
            return WriteAsync(RedisCommand.ScriptKill());
        }

        /// <summary>
        /// Load the specified Lua script into the script cache
        /// </summary>
        /// <param name="script">Lua script to load</param>
        /// <returns>SHA1 hash of script</returns>
        public Task<string> ScriptLoadAsync(string script)
        {
            return WriteAsync(RedisCommand.ScriptLoad(script));
        }
        #endregion

        #region Strings
        /// <summary>
        /// Append a value to a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to append to key</param>
        /// <returns>Length of string after append</returns>
        public Task<long> AppendAsync(string key, object value)
        {
            return WriteAsync(RedisCommand.Append(key, value));
        }

        /// <summary>
        /// Count set bits in a string
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <param name="start">Start offset</param>
        /// <param name="end">Stop offset</param>
        /// <returns>Number of bits set to 1</returns>
        public Task<long> BitCountAsync(string key, long? start = null, long? end = null)
        {
            return WriteAsync(RedisCommand.BitCount(key, start, end));
        }

        /// <summary>
        /// Perform bitwise operations between strings
        /// </summary>
        /// <param name="operation">Bit command to execute</param>
        /// <param name="destKey">Store result in destination key</param>
        /// <param name="keys">Keys to operate</param>
        /// <returns>Size of string stored in the destination key</returns>
        public Task<long> BitOpAsync(RedisBitOp operation, string destKey, params string[] keys)
        {
            return WriteAsync(RedisCommand.BitOp(operation, destKey, keys));
        }

        /// <summary>
        /// Find first bit set or clear in a string
        /// </summary>
        /// <param name="key">Key to examine</param>
        /// <param name="bit">Bit value (1 or 0)</param>
        /// <param name="start">Examine string at specified byte offset</param>
        /// <param name="end">Examine string to specified byte offset</param>
        /// <returns>Position of the first bit set to the specified value</returns>
        public Task<long> BitPosAsync(string key, bool bit, long? start = null, long? end = null)
        {
            return WriteAsync(RedisCommand.BitPos(key, bit, start, end));
        }

        /// <summary>
        /// Decrement the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after decrement</returns>
        public Task<long> DecrAsync(string key)
        {
            return WriteAsync(RedisCommand.Decr(key));
        }

        /// <summary>
        /// Decrement the integer value of a key by the given number
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="decrement">Decrement value</param>
        /// <returns>Value of key after decrement</returns>
        public Task<long> DecrByAsync(string key, long decrement)
        {
            return WriteAsync(RedisCommand.DecrBy(key, decrement));
        }

        /// <summary>
        /// Get the value of a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Value of key</returns>
        public Task<string> GetAsync(string key)
        {
            return WriteAsync(RedisCommand.Get(key));
        }

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <param name="offset">Offset of key to check</param>
        /// <returns>Bit value stored at offset</returns>
        public Task<bool> GetBitAsync(string key, uint offset)
        {
            return WriteAsync(RedisCommand.GetBit(key, offset));
        }

        /// <summary>
        /// Get a substring of the string stored at a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <param name="start">Start offset</param>
        /// <param name="end">End offset</param>
        /// <returns>Substring in the specified range</returns>
        public Task<string> GetRangeAsync(string key, long start, long end)
        {
            return WriteAsync(RedisCommand.GetRange(key, start, end));
        }

        /// <summary>
        /// Set the string value of a key and return its old value
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Old value stored at key, or null if key did not exist</returns>
        public Task<string> GetSetAsync(string key, object value)
        {
            return WriteAsync(RedisCommand.GetSet(key, value));
        }

        /// <summary>
        /// Increment the integer value of a key by one
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <returns>Value of key after increment</returns>
        public Task<long> IncrAsync(string key)
        {
            return WriteAsync(RedisCommand.Incr(key));
        }

        /// <summary>
        /// Increment the integer value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public Task<long> IncrByAsync(string key, long increment)
        {
            return WriteAsync(RedisCommand.IncrBy(key, increment));
        }

        /// <summary>
        /// Increment the float value of a key by the given amount
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="increment">Increment amount</param>
        /// <returns>Value of key after increment</returns>
        public Task<double> IncrByFloatAsync(string key, double increment)
        {
            return WriteAsync(RedisCommand.IncrByFloat(key, increment));
        }

        /// <summary>
        /// Get the values of all the given keys
        /// </summary>
        /// <param name="keys">Keys to lookup</param>
        /// <returns>Array of values at the specified keys</returns>
        public Task<string[]> MGetAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.MGet(keys));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>Status code</returns>
        public Task<string> MSetAsync(params Tuple<string, string>[] keyValues)
        {
            return WriteAsync(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>Status code</returns>
        public Task<string> MSetAsync(params string[] keyValues)
        {
            return WriteAsync(RedisCommand.MSet(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set</param>
        /// <returns>True if all keys were set</returns>
        public Task<bool> MSetNxAsync(params Tuple<string, string>[] keyValues)
        {
            return WriteAsync(RedisCommand.MSetNx(keyValues));
        }

        /// <summary>
        /// Set multiple keys to multiple values, only if none of the keys exist
        /// </summary>
        /// <param name="keyValues">Key values to set [k1, v1, k2, v2, ..]</param>
        /// <returns>True if all keys were set</returns>
        public Task<bool> MSetNxAsync(params string[] keyValues)
        {
            return WriteAsync(RedisCommand.MSetNx(keyValues));
        }

        /// <summary>
        /// Set the value and expiration in milliseconds of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="milliseconds">Expiration in milliseconds</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> PSetExAsync(string key, long milliseconds, object value)
        {
            return WriteAsync(RedisCommand.PSetEx(key, milliseconds, value));
        }

        /// <summary>
        /// Set the string value of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> SetAsync(string key, object value)
        {
            return WriteAsync(RedisCommand.Set(key, value));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expiration">Set expiration to nearest millisecond</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> SetAsync(string key, object value, TimeSpan expiration, RedisExistence? condition = null)
        {
            return WriteAsync(RedisCommand.Set(key, value, expiration, condition));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expirationSeconds">Set expiration to nearest second</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> SetAsync(string key, object value, int? expirationSeconds = null, RedisExistence? condition = null)
        {
            return WriteAsync(RedisCommand.Set(key, value, expirationSeconds, condition));
        }

        /// <summary>
        /// Set the string value of a key with atomic expiration and existence condition
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <param name="expirationMilliseconds">Set expiration to nearest millisecond</param>
        /// <param name="condition">Set key if existence condition</param>
        /// <returns>Status code, or null if condition not met</returns>
        public Task<string> SetAsync(string key, object value, long? expirationMilliseconds = null, RedisExistence? condition = null)
        {
            return WriteAsync(RedisCommand.Set(key, value, expirationMilliseconds, condition));
        }

        /// <summary>
        /// Sets or clears the bit at offset in the string value stored at key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="offset">Modify key at offset</param>
        /// <param name="value">Value to set (on or off)</param>
        /// <returns>Original bit stored at offset</returns>
        public Task<bool> SetBitAsync(string key, uint offset, bool value)
        {
            return WriteAsync(RedisCommand.SetBit(key, offset, value));
        }

        /// <summary>
        /// Set the value and expiration of a key
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="seconds">Expiration in seconds</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> SetExAsync(string key, long seconds, object value)
        {
            return WriteAsync(RedisCommand.SetEx(key, seconds, value));
        }

        /// <summary>
        /// Set the value of a key, only if the key does not exist
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="value">Value to set</param>
        /// <returns>True if key was set</returns>
        public Task<bool> SetNxAsync(string key, object value)
        {
            return WriteAsync(RedisCommand.SetNx(key, value));
        }

        /// <summary>
        /// Overwrite part of a string at key starting at the specified offset
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="offset">Start offset</param>
        /// <param name="value">Value to write at offset</param>
        /// <returns>Length of string after operation</returns>
        public Task<long> SetRangeAsync(string key, uint offset, object value)
        {
            return WriteAsync(RedisCommand.SetRange(key, offset, value));
        }

        /// <summary>
        /// Get the length of the value stored in a key
        /// </summary>
        /// <param name="key">Key to lookup</param>
        /// <returns>Length of string at key</returns>
        public Task<long> StrLenAsync(string key)
        {
            return WriteAsync(RedisCommand.StrLen(key));
        }
        #endregion

        #region Server
        /// <summary>
        /// Asyncronously rewrite the append-only file
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> BgRewriteAofAsync()
        {
            return WriteAsync(RedisCommand.BgRewriteAof());
        }

        /// <summary>
        /// Asynchronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> BgSaveAsync()
        {
            return WriteAsync(RedisCommand.BgSave());
        }

        /// <summary>
        /// Get the current connection name
        /// </summary>
        /// <returns>Connection name</returns>
        public Task<string> ClientGetNameAsync()
        {
            return WriteAsync(RedisCommand.ClientGetName());
        }

        /// <summary>
        /// Kill the connection of a client
        /// </summary>
        /// <param name="ip">Client IP returned from CLIENT LIST</param>
        /// <param name="port">Client port returned from CLIENT LIST</param>
        /// <returns>Status code</returns>
        public Task<string> ClientKillAsync(string ip, int port)
        {
            return WriteAsync(RedisCommand.ClientKill(ip, port));
        }

        public Task<long> ClientKillAsync(string addr = null, string id = null, string type = null, bool? skipMe = null)
        {
            return WriteAsync(RedisCommand.ClientKill(addr, id, type, skipMe));
        }

        /// <summary>
        /// Get the list of client connections
        /// </summary>
        /// <returns>Formatted string of clients</returns>
        public Task<string> ClientListAsync()
        {
            return WriteAsync(RedisCommand.ClientList());
        }

        public Task<string> ClientPauseAsync(int milliseconds)
        {
            return WriteAsync(RedisCommand.ClientPause(milliseconds));
        }

        public Task<string> ClientPauseAsync(TimeSpan timeout)
        {
            return WriteAsync(RedisCommand.ClientPause(timeout));
        }

        /// <summary>
        /// Set the current connection name
        /// </summary>
        /// <param name="connectionName">Name of connection (no spaces)</param>
        /// <returns>Status code</returns>
        public Task<string> ClientSetNameAsync(string connectionName)
        {
            return WriteAsync(RedisCommand.ClientSetName(connectionName));
        }

        /// <summary>
        /// Get the value of a configuration paramter
        /// </summary>
        /// <param name="parameter">Configuration parameter to lookup</param>
        /// <returns>Configuration value</returns>
        public Task<Tuple<string, string>[]> ConfigGetAsync(string parameter)
        {
            return WriteAsync(RedisCommand.ConfigGet(parameter));
        }

        /// <summary>
        /// Reset the stats returned by INFO
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> ConfigResetStatAsync()
        {
            return WriteAsync(RedisCommand.ConfigResetStat());
        }

        public Task<string> ConfigRewriteAsync()
        {
            return WriteAsync(RedisCommand.ConfigRewrite());
        }

        /// <summary>
        /// Set a configuration parameter to the given value
        /// </summary>
        /// <param name="parameter">Parameter to set</param>
        /// <param name="value">Value to set</param>
        /// <returns>Status code</returns>
        public Task<string> ConfigSetAsync(string parameter, string value)
        {
            return WriteAsync(RedisCommand.ConfigSet(parameter, value));
        }

        /// <summary>
        /// Return the number of keys in the selected database
        /// </summary>
        /// <returns>Number of keys</returns>
        public Task<long> DbSizeAsync()
        {
            return WriteAsync(RedisCommand.DbSize());
        }

        /// <summary>
        /// Make the server crash :(
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> DebugSegFaultAsync()
        {
            return WriteAsync(RedisCommand.DebugSegFault());
        }

        /// <summary>
        /// Remove all keys from all databases
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> FlushAllAsync()
        {
            return WriteAsync(RedisCommand.FlushAll());
        }

        /// <summary>
        /// Remove all keys from the current database
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> FlushDbAsync()
        {
            return WriteAsync(RedisCommand.FlushDb());
        }

        /// <summary>
        /// Get information and statistics about the server
        /// </summary>
        /// <param name="section">all|default|server|clients|memory|persistence|stats|replication|cpu|commandstats|cluster|keyspace</param>
        /// <returns>Formatted string</returns>
        public Task<string> InfoAsync(string section = null)
        {
            return WriteAsync(RedisCommand.Info());
        }

        /// <summary>
        /// Get the timestamp of the last successful save to disk
        /// </summary>
        /// <returns>Date of last save</returns>
        public Task<DateTime> LastSaveAsync()
        {
            return WriteAsync(RedisCommand.LastSave());
        }

        public Task<RedisRole> RoleAsync()
        {
            return WriteAsync(RedisCommand.Role());
        }

        /// <summary>
        /// Syncronously save the dataset to disk
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> SaveAsync()
        {
            return WriteAsync(RedisCommand.Save());
        }

        /// <summary>
        /// Syncronously save the dataset to disk an then shut down the server
        /// </summary>
        /// <param name="save">Force a DB saving operation even if no save points are configured</param>
        /// <returns>Status code</returns>
        public Task<string> ShutdownAsync(bool? save = null)
        {
            return WriteAsync(RedisCommand.Shutdown());
        }

        /// <summary>
        /// Make the server a slave of another instance or promote it as master
        /// </summary>
        /// <param name="host">Master host</param>
        /// <param name="port">master port</param>
        /// <returns>Status code</returns>
        public Task<string> SlaveOfAsync(string host, int port)
        {
            return WriteAsync(RedisCommand.SlaveOf(host, port));
        }

        /// <summary>
        /// Turn off replication, turning the Redis server into a master
        /// </summary>
        /// <returns>Status code</returns>
        public Task<string> SlaveOfNoOneAsync()
        {
            return WriteAsync(RedisCommand.SlaveOfNoOne());
        }

        public Task<RedisSlowLogEntry[]> SlowLogGetAsync(long? count = null)
        {
            return WriteAsync(RedisCommand.SlowLogGet(count));
        }

        public Task<long> SlowLogLenAsync()
        {
            return WriteAsync(RedisCommand.SlowLogLen());
        }
        public Task<string> SlowLogResetAsync()
        {
            return WriteAsync(RedisCommand.SlowLogReset());
        }

        /// <summary>
        /// Internal command used for replication
        /// </summary>
        /// <returns>Byte array of Redis sync data</returns>
        public Task<byte[]> SyncAsync()
        {
            return WriteAsync(RedisCommand.Sync());
        }

        /// <summary>
        /// Return the current server time
        /// </summary>
        /// <returns>Server time</returns>
        public Task<DateTime> TimeAsync()
        {
            return WriteAsync(RedisCommand.Time());
        }
        #endregion

        #region Transactions
        public Task<string> MultiAsyncAsync()
        {
            return _transaction.StartAsync();
        }

        public Task<string> DiscardAsyncAsync()
        {
            return _transaction.AbortAsync();
        }

        public Task<object[]> ExecAsyncAsync()
        {
            return _transaction.ExecuteAsync();
        }

        public Task<string> UnwatchAsyncAsync()
        {
            return WriteAsync(RedisCommand.Unwatch());
        }

        public Task<string> WatchAsyncAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.Watch(keys));
        }
        #endregion

        #region HyperLogLog
        /// <summary>
        /// Adds the specified elements to the specified HyperLogLog.
        /// </summary>
        /// <param name="key">Key to update</param>
        /// <param name="elements">Elements to add</param>
        /// <returns>1 if at least 1 HyperLogLog internal register was altered. 0 otherwise.</returns>
        public Task<bool> PfAddAsync(string key, params object[] elements)
        {
            return WriteAsync(RedisCommand.PfAdd(key, elements));
        }
        /// <summary>
        /// Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s)
        /// </summary>
        /// <param name="keys">One or more HyperLogLog keys to examine</param>
        /// <returns>Approximated number of unique elements observed via PFADD</returns>
        public Task<long> PfCountAsync(params string[] keys)
        {
            return WriteAsync(RedisCommand.PfCount(keys));
        }
        /// <summary>
        /// Merge N different HyperLogLogs into a single key.
        /// </summary>
        /// <param name="destKey">Where to store the merged HyperLogLogs</param>
        /// <param name="sourceKeys">The HyperLogLogs keys that will be combined</param>
        /// <returns>Status code</returns>
        public Task<string> PfMergeAsync(string destKey, params string[] sourceKeys)
        {
            return WriteAsync(RedisCommand.PfMerge(destKey, sourceKeys));
        }
        #endregion
    }
}
