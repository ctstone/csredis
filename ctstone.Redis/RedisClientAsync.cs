using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
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
            RedisClient client = new RedisClient(Host, Port, 0);
            if (_password != null)
                client.Auth(_password);
            return client;
        }

        /// <summary>
        /// Get a thread-safe, reusable subscription channel.
        /// </summary>
        /// <returns>A reusable subscription channel</returns>
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
        public Task<bool> Expire(string key, int seconds)
        {
            return Write(RedisCommand.Expire(key, seconds));
        }

        /// <summary>
        /// Set a key's time to live in seconds
        /// </summary>
        /// <param name="key">Key to modify</param>
        /// <param name="seconds">Expiration in seconds</param>
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
        /// <param name="timeoutMilliseconds">Timeout in milliseconds</param>
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
        /// <param name="timeoutMilliseconds">Timeout in milliseconds</param>
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

        
        public Task<string> ObjectEncoding(params string[] arguments)
        {
            return Write(RedisCommand.ObjectEncoding(arguments));
        }

        
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
        public Task<long> HDel(string key, params string[] fields)
        {
            return Write(RedisCommand.HDel(key, fields));
        }
        public Task<bool> HExists(string key, string field)
        {
            return Write(RedisCommand.HExists(key, field));
        }
        public Task<string> HGet(string key, string field)
        {
            return Write(RedisCommand.HGet(key, field));
        }
        public Task<T> HGetAll<T>(string key)
            where T : new()
        {
            return Write(RedisCommand.HGetAll<T>(key));
        }
        public Task<Dictionary<string, string>> HGetAll(string key)
        {
            return Write(RedisCommand.HGetAll(key));
        }
        public Task<long> HIncrBy(string key, string field, long increment)
        {
            return Write(RedisCommand.HIncrBy(key, field, increment));
        }
        public Task<double> HIncrByFloat(string key, string field, double increment)
        {
            return Write(RedisCommand.HIncrByFloat(key, field, increment));
        }
        public Task<string[]> HKeys(string key)
        {
            return Write(RedisCommand.HKeys(key));
        }
        public Task<long> HLen(string key)
        {
            return Write(RedisCommand.HLen(key));
        }
        public Task<string[]> HMGet(string key, params string[] fields)
        {
            return Write(RedisCommand.HMGet(key, fields));
        }
        public Task<string> HMSet(string key, Dictionary<string, string> dict)
        {
            return Write(RedisCommand.HMSet(key, dict));
        }
        public Task<string> HMSet<T>(string key, T obj)
            where T : new()
        {
            return Write(RedisCommand.HMSet<T>(key, obj));
        }
        public Task<string> HMSet(string key, params string[] keyValues)
        {
            return Write(RedisCommand.HMSet(key, keyValues));
        }
        public Task<bool> HSet(string key, string field, object value)
        {
            return Write(RedisCommand.HSet(key, field, value));
        }
        public Task<bool> HSetNx(string key, string field, object value)
        {
            return Write(RedisCommand.HSetNx(key, field, value));
        }
        public Task<string[]> HVals(string key)
        {
            return Write(RedisCommand.HVals(key));
        }
        #endregion

        #region Lists
        public Task<string> LIndex(string key, long index)
        {
            return Write(RedisCommand.LIndex(key, index));
        }
        public Task<long> LInsert(string key, RedisInsert insertType, string pivot, object value)
        {
            return Write(RedisCommand.LInsert(key, insertType, pivot, value));
        }
        public Task<long> LLen(string key)
        {
            return Write(RedisCommand.LLen(key));
        }
        public Task<string> LPop(string key)
        {
            return Write(RedisCommand.LPop(key));
        }
        public Task<long> LPush(string key, params object[] values)
        {
            return Write(RedisCommand.LPush(key, values));
        }
        public Task<long> LPushX(string key, object value)
        {
            return Write(RedisCommand.LPushX(key, value));
        }
        public Task<string[]> LRange(string key, long start, long stop)
        {
            return Write(RedisCommand.LRange(key, start, stop));
        }
        public Task<long> LRem(string key, long count, object value)
        {
            return Write(RedisCommand.LRem(key, count, value));
        }
        public Task<string> LSet(string key, long index, object value)
        {
            return Write(RedisCommand.LSet(key, index, value));
        }
        public Task<string> LTrim(string key, long start, long stop)
        {
            return Write(RedisCommand.LTrim(key, start, stop));
        }
        public Task<string> RPop(string key)
        {
            return Write(RedisCommand.RPop(key));
        }
        public Task<string> RPopLPush(string source, string destination)
        {
            return Write(RedisCommand.RPopLPush(source, destination));
        }
        public Task<long> RPush(string key, params object[] values)
        {
            return Write(RedisCommand.RPush(key, values));
        }
        public Task<long> RPushX(string key, params object[] values)
        {
            return Write(RedisCommand.RPushX(key, values));
        }
        #endregion

        #region Sets
        public Task<long> SAdd(string key, params object[] members)
        {
            return Write(RedisCommand.SAdd(key, members));
        }
        public Task<long> SCard(string key)
        {
            return Write(RedisCommand.SCard(key));
        }
        public Task<string[]> SDiff(params string[] keys)
        {
            return Write(RedisCommand.SDiff(keys));
        }
        public Task<long> SDiffStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SDiffStore(destination, keys));
        }
        public Task<string[]> SInter(params string[] keys)
        {
            return Write(RedisCommand.SInter(keys));
        }
        public Task<long> SInterStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SInterStore(destination, keys));
        }
        public Task<bool> SIsMember(string key, object member)
        {
            return Write(RedisCommand.SIsMember(key, member));
        }
        public Task<string[]> SMembers(string key)
        {
            return Write(RedisCommand.SMembers(key));
        }
        public Task<bool> SMove(string source, string destination, object member)
        {
            return Write(RedisCommand.SMove(source, destination, member));
        }
        public Task<string> SPop(string key)
        {
            return Write(RedisCommand.SPop(key));
        }
        public Task<string> SRandMember(string key)
        {
            return Write(RedisCommand.SRandMember(key));
        }
        public Task<string[]> SRandMember(string key, long count)
        {
            return Write(RedisCommand.SRandMember(key, count));
        }
        public Task<long> SRem(string key, params object[] keys)
        {
            return Write(RedisCommand.SRem(key, keys));
        }
        public Task<string[]> SUnion(params string[] keys)
        {
            return Write(RedisCommand.SUnion(keys));
        }
        public Task<long> SUnionStore(string destination, params string[] keys)
        {
            return Write(RedisCommand.SUnionStore(destination, keys));
        }
        #endregion

        #region Sorted Sets
        public Task<long> ZAdd<TScore, TMember>(string key, params Tuple<TScore, TMember>[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }
        public Task<long> ZAdd(string key, params string[] memberScores)
        {
            return Write(RedisCommand.ZAdd(key, memberScores));
        }
        public Task<long> ZCard(string key)
        {
            return Write(RedisCommand.ZCard(key));
        }
        public Task<long> ZCount(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return Write(RedisCommand.ZCount(key, min, max, exclusiveMin, exclusiveMax));
        }
        public Task<double> ZIncrBy(string key, double increment, string member)
        {
            return Write(RedisCommand.ZIncrBy(key, increment, member));
        }
        public Task<long> ZInterStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return Write(RedisCommand.ZInterStore(destination, weights, aggregate, keys));
        }
        public Task<string[]> ZRange(string key, long start, long stop, bool withScores = false)
        {
            return Write(RedisCommand.ZRange(key, start, stop, withScores));
        }
        public Task<string[]> ZRangeByScore(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRangeByScore(key, min, max, withScores, exclusiveMin, exclusiveMax, offset, count));
        }
        public Task<long?> ZRank(string key, string member)
        {
            return Write(RedisCommand.ZRank(key, member));
        }
        public Task<long> ZRem(string key, params object[] members)
        {
            return Write(RedisCommand.ZRem(key, members));
        }
        public Task<long> ZRemRangeByRank(string key, long start, long stop)
        {
            return Write(RedisCommand.ZRemRangeByRank(key, start, stop));
        }
        public Task<long> ZRemRangeByScore(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            return Write(RedisCommand.ZRemRangeByScore(key, min, max, exclusiveMin, exclusiveMax));
        }
        public Task<string[]> ZRevRange(string key, long start, long stop, bool withScores = false)
        {
            return Write(RedisCommand.ZRevRange(key, start, stop, withScores));
        }
        public Task<string[]> ZRevRangeByScore(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            return Write(RedisCommand.ZRevRangeByScore(key, max, min, withScores, exclusiveMax, exclusiveMin, offset, count));
        }
        public Task<long?> ZRevRank(string key, string member)
        {
            return Write(RedisCommand.ZRevRank(key, member));
        }
        public Task<double?> ZScore(string key, string member)
        {
            return Write(RedisCommand.ZScore(key, member));
        }
        public Task<long> ZUnionStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            return Write(RedisCommand.ZUnionStore(destination, weights, aggregate, keys));
        }
        #endregion

        #region Pub/Sub
        public Task<long> Publish(string channel, string message)
        {
            return Write(RedisCommand.Publish(channel, message));
        }
        #endregion

        #region Scripting
        public Task<object> Eval(string script, string[] keys, params string[] arguments)
        {
            return Write(RedisCommand.Eval(script, keys, arguments));
        }
        public Task<object> EvalSHA(string sha1, string[] keys, params string[] arguments)
        {
            return Write(RedisCommand.EvalSHA(sha1, keys, arguments));
        }
        public Task<bool[]> ScriptExists(params string[] scripts)
        {
            return Write(RedisCommand.ScriptExists(scripts));
        }
        public Task<string> ScriptFlush()
        {
            return Write(RedisCommand.ScriptFlush());
        }
        public Task<string> ScriptKill()
        {
            return Write(RedisCommand.ScriptKill());
        }
        public Task<string> ScriptLoad(string script)
        {
            return Write(RedisCommand.ScriptLoad(script));
        }
        #endregion

        #region Strings
        public Task<long> Append(string key, object value)
        {
            return Write(RedisCommand.Append(key, value));
        }
        public Task<long> BitCount(string key, long? start = null, long? end = null)
        {
            return Write(RedisCommand.BitCount(key, start, end));
        }
        public Task<long> BitOp(RedisBitOp operation, string destKey, params string[] keys)
        {
            return Write(RedisCommand.BitOp(operation, destKey, keys));
        }
        public Task<long> Decr(string key)
        {
            return Write(RedisCommand.Decr(key));
        }
        public Task<long> DecrBy(string key, long decrement)
        {
            return Write(RedisCommand.DecrBy(key, decrement));
        }
        public Task<string> Get(string key)
        {
            return Write(RedisCommand.Get(key));
        }
        public Task<bool> GetBit(string key, uint offset)
        {
            return Write(RedisCommand.GetBit(key, offset));
        }
        public Task<string> GetRange(string key, long start, long end)
        {
            return Write(RedisCommand.GetRange(key, start, end));
        }
        public Task<string> GetSet(string key, object value)
        {
            return Write(RedisCommand.GetSet(key, value));
        }
        public Task<long> Incr(string key)
        {
            return Write(RedisCommand.Incr(key));
        }
        public Task<long> IncrBy(string key, long increment)
        {
            return Write(RedisCommand.IncrBy(key, increment));
        }
        public Task<double> IncrByFloat(string key, double increment)
        {
            return Write(RedisCommand.IncrByFloat(key, increment));
        }
        public Task<string[]> MGet(params string[] keys)
        {
            return Write(RedisCommand.MGet(keys));
        }
        public Task<string> MSet(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }
        public Task<string> MSet(params string[] keyValues)
        {
            return Write(RedisCommand.MSet(keyValues));
        }
        public Task<bool> MSetNx(params Tuple<string, string>[] keyValues)
        {
            return Write(RedisCommand.MSetNx(keyValues));
        }
        public Task<bool> MSetNx(params string[] keyValues)
        {
            return Write(RedisCommand.MSetNx(keyValues));
        }
        public Task<string> PSetEx(string key, long milliseconds, object value)
        {
            return Write(RedisCommand.PSetEx(key, milliseconds, value));
        }
        public Task<string> Set(string key, object value)
        {
            return Write(RedisCommand.Set(key, value));
        }
        public Task<string> Set(string key, object value, TimeSpan expiration, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expiration, condition));
        }
        public Task<string> Set(string key, object value, int? expirationSeconds = null, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expirationSeconds, condition));
        }
        public Task<string> Set(string key, object value, long? expirationMilliseconds = null, RedisExistence? condition = null)
        {
            return Write(RedisCommand.Set(key, value, expirationMilliseconds, condition));
        }
        public Task<bool> SetBit(string key, uint offset, bool value)
        {
            return Write(RedisCommand.SetBit(key, offset, value));
        }
        public Task<string> SetEx(string key, long seconds, object value)
        {
            return Write(RedisCommand.SetEx(key, seconds, value));
        }
        public Task<bool> SetNx(string key, object value)
        {
            return Write(RedisCommand.SetNx(key, value));
        }
        public Task<long> SetRange(string key, uint offset, object value)
        {
            return Write(RedisCommand.SetRange(key, offset, value));
        }
        public Task<long> StrLen(string key)
        {
            return Write(RedisCommand.StrLen(key));
        }
        #endregion

        #region Server
        public Task<string> BgRewriteAof()
        {
            return Write(RedisCommand.BgRewriteAof());
        }
        public Task<string> BgSave()
        {
            return Write(RedisCommand.BgSave());
        }
        public Task<string> ClientGetName()
        {
            return Write(RedisCommand.ClientGetName());
        }
        public Task<string> ClientKill(string ip, int port)
        {
            return Write(RedisCommand.ClientKill(ip, port));
        }
        public Task<string> ClientList()
        {
            return Write(RedisCommand.ClientList());
        }
        public Task<string> ClientSetName(string connectionName)
        {
            return Write(RedisCommand.ClientSetName(connectionName));
        }
        public Task<string> ConfigGet(string parameter)
        {
            return Write(RedisCommand.ConfigGet(parameter));
        }
        public Task<string> ConfigResetStat()
        {
            return Write(RedisCommand.ConfigResetStat());
        }
        public Task<string> ConfigSet(string parameter, string value)
        {
            return Write(RedisCommand.ConfigSet(parameter, value));
        }
        public Task<long> DbSize()
        {
            return Write(RedisCommand.DbSize());
        }
        public Task<string> DebugObject(string key)
        {
            return Write(RedisCommand.DebugObject(key));
        }
        public Task<string> DebugSegFault()
        {
            return Write(RedisCommand.DebugSegFault());
        }
        public Task<string> FlushAll()
        {
            return Write(RedisCommand.FlushAll());
        }
        public Task<string> FlushDb()
        {
            return Write(RedisCommand.FlushDb());
        }
        public Task<string> Info(string section = null)
        {
            return Write(RedisCommand.Info());
        }
        public Task<DateTime> LastSave()
        {
            return Write(RedisCommand.LastSave());
        }
        public Task<string> Save()
        {
            return Write(RedisCommand.Save());
        }
        public Task<string> Shutdown(bool? save = null)
        {
            return Write(RedisCommand.Shutdown());
        }
        public Task<string> SlaveOf(string host, int port)
        {
            return Write(RedisCommand.SlaveOf(host, port));
        }
        public Task<string> SlaveOfNoOne()
        {
            return Write(RedisCommand.SlaveOfNoOne());
        }
        public Task<object> SlowLog(RedisSlowLog subCommand, string argument = null)
        {
            return Write(RedisCommand.SlowLog(subCommand, argument));
        }
        public Task<byte[]> Sync()
        {
            return Write(RedisCommand.Sync());
        }
        public Task<DateTime> Time()
        {
            return Write(RedisCommand.Time());
        }
        #endregion

        private Task<T> Write<T>(RedisCommand<T> command)
        {
            if (!_connection.Connected)
                throw new InvalidOperationException("RedisClientAsync is not connected");

            return _connection.CallAsync(command.Parser, command.Command, command.Arguments);
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
        }
    }
}
