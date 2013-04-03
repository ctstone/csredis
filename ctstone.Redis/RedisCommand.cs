using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace ctstone.Redis
{
    static class RedisCommand
    {
        #region Connection
        public static RedisStatus Auth(string password)
        {
            return new RedisStatus("AUTH", password);
        }
        public static RedisString Echo(string message)
        {
            return new RedisString("ECHO", message);
        }
        public static RedisStatus Ping()
        {
            return new RedisStatus("PING");
        }
        public static RedisStatus Quit()
        {
            return new RedisStatus("QUIT");
        }
        public static RedisStatus Select(uint index)
        {
            return new RedisStatus("SELECT", index);
        }
        #endregion

        #region Keys
        public static RedisInt Del(params string[] keys)
        {
            return new RedisInt("DEL", keys);
        }
        public static RedisBytes Dump(string key)
        {
            return new RedisBytes("DUMP", key);
        }
        public static RedisBool Exists(string key)
        {
            return new RedisBool("EXISTS", key);
        }
        public static RedisBool Expire(string key, TimeSpan expiration)
        {
            return new RedisBool("EXPIRE", key, (int)expiration.TotalSeconds);
        }
        public static RedisBool Expire(string key, int seconds)
        {
            return new RedisBool("EXPIRE", key, seconds);
        }
        public static RedisBool ExpireAt(string key, DateTime expirationDate)
        {
            return new RedisBool("EXPIREAT", key, (int)(expirationDate - RedisArgs.Epoch).TotalSeconds);
        }
        public static RedisBool ExpireAt(string key, int timestamp)
        {
            return new RedisBool("EXPIREAT", key, timestamp);
        }
        public static RedisStrings Keys(string pattern)
        {
            return new RedisStrings("KEYS", pattern);
        }
        public static RedisStatus Migrate(string host, int port, string key, int destinationDb, int timeoutMilliseconds)
        {
            return new RedisStatus("MIGRATE", host, port, key, destinationDb, timeoutMilliseconds);
        }
        public static RedisStatus Migrate(string host, int port, string key, int destinationDb, TimeSpan timeout)
        {
            return Migrate(host, port, key, destinationDb, (int)timeout.TotalMilliseconds);
        }
        public static RedisBool Move(string key, int database)
        {
            return new RedisBool("MOVE", key, database);
        }
        public static RedisString ObjectEncoding(params string[] arguments)
        {
            string[] args = RedisArgs.Concat("ENCODING", arguments);
            return new RedisString("OBJECT", args);
        }
        public static RedisIntNull Object(RedisObjectSubCommand subCommand, params string[] arguments)
        {
            object[] args = RedisArgs.Concat(subCommand.ToString(), arguments);
            return new RedisIntNull("OBJECT", args);
        }
        public static RedisBool Persist(string key)
        {
            return new RedisBool("PERSIST", key);
        }
        public static RedisBool PExpire(string key, TimeSpan expiration)
        {
            return new RedisBool("PEXPIRE", key, (int)expiration.TotalSeconds);
        }
        public static RedisBool PExpire(string key, long milliseconds)
        {
            return new RedisBool("PEXPIRE", key, milliseconds);
        }
        public static RedisBool PExpireAt(string key, DateTime date)
        {
            return new RedisBool("PEXPIREAT", key, (long)(date - RedisArgs.Epoch).TotalMilliseconds);
        }
        public static RedisBool PExpireAt(string key, long timestamp)
        {
            return new RedisBool("PEXPIREAT", key, timestamp);
        }
        public static RedisInt PTtl(string key)
        {
            return new RedisInt("TTL", key);
        }
        public static RedisString RandomKey()
        {
            return new RedisString("RANDOMKEY");
        }
        public static RedisStatus Rename(string key, string newKey)
        {
            return new RedisStatus("RENAME", key, newKey);
        }
        public static RedisBool RenameNx(string key, string newKey)
        {
            return new RedisBool("RENAMENX", key, newKey);
        }
        public static RedisStatus Restore(string key, long ttl, string serializedValue)
        {
            return new RedisStatus("RESTORE", key, ttl, serializedValue);
        }
        public static RedisStringHashes Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, bool? isHash = null, params string[] get)
        {
            List<string> args = new List<string>();
            args.Add(key);
            if (by != null)
                args.AddRange(new[] { "BY", by });
            if (offset.HasValue && count.HasValue)
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            foreach (var pattern in get)
                args.AddRange(new[] { "GET", pattern });
            if (dir.HasValue)
                args.Add(dir.ToString().ToUpper());
            if (isAlpha.HasValue && isAlpha.Value)
                args.Add("ALPHA");
            return new RedisStringHashes("SORT",get,  args.ToArray());
        }
        public static RedisStrings Sort(string key, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            List<string> args = new List<string>();
            args.Add(key);
            if (by != null)
                args.AddRange(new[] { "BY", by });
            if (offset.HasValue && count.HasValue)
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            foreach (var pattern in get)
                args.AddRange(new[] { "GET", pattern });
            if (dir.HasValue)
                args.Add(dir.ToString().ToUpper());
            if (isAlpha.HasValue && isAlpha.Value)
                args.Add("ALPHA");
            return new RedisStrings("SORT", args.ToArray());
        }
        public static RedisInt SortAndStore(string key, string destination, long? offset = null, long? count = null, string by = null, RedisSortDir? dir = null, bool? isAlpha = null, params string[] get)
        {
            List<string> args = new List<string>();
            args.Add(key);
            if (by != null)
                args.AddRange(new[] { "BY", by });
            if (offset.HasValue && count.HasValue)
                args.AddRange(new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });
            foreach (var pattern in get)
                args.AddRange(new[] { "GET", pattern });
            if (dir.HasValue)
                args.Add(dir.ToString().ToUpper());
            if (isAlpha.HasValue && isAlpha.Value)
                args.Add("ALPHA");
            args.AddRange(new[] { "STORE", destination });
            return new RedisInt("SORT", args.ToArray());
        }
        public static RedisInt Ttl(string key)
        {
            return new RedisInt("TTL", key);
        }
        public static RedisStatus Type(string key)
        {
            return new RedisStatus("TYPE", key);
        }
        #endregion

        #region Hashes
        public static RedisInt HDel(string key, params string[] fields)
        {
            string[] args = RedisArgs.Concat(key, fields);
            return new RedisInt("HDEL", args);
        }
        public static RedisBool HExists(string key, string field)
        {
            return new RedisBool("HEXISTS", key, field);
        }
        public static RedisString HGet(string key, string field)
        {
            return new RedisString("HGET", key, field);
        }
        public static RedisHash<T> HGetAll<T>(string key)
            where T : new()
        {
            return new RedisHash<T>("HGETALL", key);
        }
        public static RedisHash HGetAll(string key)
        {
            return new RedisHash("HGETALL", key);
        }
        public static RedisInt HIncrBy(string key, string field, long increment)
        {
            return new RedisInt("HINCRBY", key, field, increment);
        }
        public static RedisFloat HIncrByFloat(string key, string field, double increment)
        {
            return new RedisFloat("HINCRBYFLOAT", key, field, increment);
        }
        public static RedisStrings HKeys(string key)
        {
            return new RedisStrings("HKEYS", key);
        }
        public static RedisInt HLen(string key)
        {
            return new RedisInt("HLEN", key);
        }
        public static RedisStrings HMGet(string key, params string[] fields)
        {
            string[] args = RedisArgs.Concat(key, fields);
            return new RedisStrings("HMGET", args);
        }
        public static RedisStatus HMSet(string key, Dictionary<string, string> dict)
        {
            List<object> args = new List<object> { key };
            foreach (var keyValue in dict)
            {
                if (keyValue.Key != null && keyValue.Value != null)
                    args.AddRange(new[] { keyValue.Key, keyValue.Value });
            }
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisStatus HMSet<T>(string key, T obj)
        {
            List<object> args = new List<object> { key };
            PropertyInfo[] props = typeof(T).GetProperties();
            foreach (var prop in props)
            {
                object value = prop.GetValue(obj, null);
                if (prop.CanRead && value != null)
                    args.AddRange(new[] { prop.Name, prop.GetValue(obj, null) });
            }
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisStatus HMSet(string key, params string[] keyValues)
        {
            List<object> args = new List<object> { key };
            for (int i = 0; i < keyValues.Length; i += 2)
            {
                if (keyValues[i] != null && keyValues[i + 1] != null)
                    args.AddRange(new[] { keyValues[i], keyValues[i + 1] });
            }
            return new RedisStatus("HMSET", args.ToArray());
        }
        public static RedisBool HSet(string key, string field, object value)
        {
            return new RedisBool("HSET", key, field, value);
        }
        public static RedisBool HSetNx(string key, string field, object value)
        {
            return new RedisBool("HSETNX", key, field, value);
        }
        public static RedisStrings HVals(string key)
        {
            return new RedisStrings("HVALS", key);
        }
        #endregion

        #region Lists
        public static RedisTuple BLPopWithKey(int timeout, params string[] keys)
        {
            string[] args = RedisArgs.Concat(keys, new object[] { timeout });
            return new RedisTuple("BLPOP", args);
        }
        public static RedisTuple BLPopWithKey(TimeSpan timeout, params string[] keys)
        {
            return BLPopWithKey((int)timeout.TotalSeconds, keys);
        }
        public static RedisValue BLPop(int timeout, params string[] keys)
        {
            string[] args = RedisArgs.Concat(keys, new object[] { timeout });
            return new RedisValue("BLPOP", args);
        }
        public static RedisValue BLPop(TimeSpan timeout, params string[] keys)
        {
            return BLPop((int)timeout.TotalSeconds, keys);
        }
        public static RedisTuple BRPopWithKey(int timeout, params string[] keys)
        {
            string[] args = RedisArgs.Concat(keys, new object[] { timeout });
            return new RedisTuple("BRPOP", args);
        }
        public static RedisTuple BRPopWithKey(TimeSpan timeout, params string[] keys)
        {
            return BRPopWithKey((int)timeout.TotalSeconds, keys);
        }
        public static RedisValue BRPop(int timeout, params string[] keys)
        {
            string[] args = RedisArgs.Concat(keys, new object[] { timeout });
            return new RedisValue("BRPOP", args);
        }
        public static RedisValue BRPop(TimeSpan timeout, params string[] keys)
        {
            return BRPop((int)timeout.TotalSeconds, keys);
        }
        public static RedisStringNull BRPopLPush(string source, string destination, int timeout)
        {
            return new RedisStringNull("BRPOPLPUSH", source, destination, timeout);
        }
        public static RedisStringNull BRPopLPush(string source, string destination, TimeSpan timeout)
        {
            return BRPopLPush(source, destination, (int)timeout.TotalSeconds);
        }
        public static RedisString LIndex(string key, long index)
        {
            return new RedisString("LINDEX", key, index);
        }
        public static RedisInt LInsert(string key, RedisInsert insertType, string pivot, object value)
        {
            return new RedisInt("LINSERT", key, insertType.ToString().ToUpper(), pivot, value);
        }
        public static RedisInt LLen(string key)
        {
            return new RedisInt("LLEN", key);
        }
        public static RedisString LPop(string key)
        {
            return new RedisString("LPOP", key);
        }
        public static RedisInt LPush(string key, params object[] values)
        {
            string[] args = RedisArgs.Concat(new[] { key }, values);
            return new RedisInt("LPUSH", args);
        }
        public static RedisInt LPushX(string key, object value)
        {
            return new RedisInt("LPUSHX", key, value);
        }
        public static RedisStrings LRange(string key, long start, long stop)
        {
            return new RedisStrings("LRANGE", key, start, stop);
        }
        public static RedisInt LRem(string key, long count, object value)
        {
            return new RedisInt("LREM", key, count, value);
        }
        public static RedisStatus LSet(string key, long index, object value)
        {
            return new RedisStatus("LSET", key, index, value);
        }
        public static RedisStatus LTrim(string key, long start, long stop)
        {
            return new RedisStatus("LTRIM", key, start, stop);
        }
        public static RedisString RPop(string key)
        {
            return new RedisString("RPOP", key);
        }
        public static RedisString RPopLPush(string source, string destination)
        {
            return new RedisString("RPOPLPUSH", source, destination);
        }
        public static RedisInt RPush(string key, params object[] values)
        {
            string[] args = RedisArgs.Concat(key, values);
            return new RedisInt("RPUSH", args);
        }
        public static RedisInt RPushX(string key, params object[] values)
        {
            string[] args = RedisArgs.Concat(key, values);
            return new RedisInt("RPUSHX", args);
        }
        #endregion

        #region Sets
        public static RedisInt SAdd(string key, params object[] members)
        {
            object[] args = RedisArgs.Concat(key, members);
            return new RedisInt("SADD", args);
        }
        public static RedisInt SCard(string key)
        {
            return new RedisInt("SCARD", key);
        }
        public static RedisStrings SDiff(params string[] keys)
        {
            return new RedisStrings("SDIFF", keys);
        }
        public static RedisInt SDiffStore(string destination, params string[] keys)
        {
            object[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SDIFFSTORE", args);
        }
        public static RedisStrings SInter(params string[] keys)
        {
            return new RedisStrings("SINTER", keys);
        }
        public static RedisInt SInterStore(string destination, params string[] keys)
        {
            object[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SINTERSTORE", args);
        }
        public static RedisBool SIsMember(string key, object member) 
        {
            return new RedisBool("SISMEMBER", key, member);
        }
        public static RedisStrings SMembers(string key)
        {
            return new RedisStrings("SMEMBERS", key);
        }
        public static RedisBool SMove(string source, string destination, object member)
        {
            return new RedisBool("SMOVE", source, destination, member);
        }
        public static RedisString SPop(string key)
        {
            return new RedisString("SPOP", key);
        }
        public static RedisString SRandMember(string key)
        {
            return new RedisString("SRANDMEMBER", key);
        }
        public static RedisStrings SRandMember(string key, long count)
        {
            return new RedisStrings("SRANDMEMBER", key, count);
        }
        public static RedisInt SRem(string key, params object[] members) 
        {
            object[] args = RedisArgs.Concat(key, members);
            return new RedisInt("SREM", args);
        }
        public static RedisStrings SUnion(params string[] keys)
        {
            return new RedisStrings("SUNION", keys);
        }
        public static RedisInt SUnionStore(string destination, params string[] keys)
        {
            string[] args = RedisArgs.Concat(destination, keys);
            return new RedisInt("SUNIONSTORE", args);
        }
        #endregion

        #region Sorted Sets
        public static RedisInt ZAdd(string key, params Tuple<double, string>[] memberScores)
        {
            object[] args = RedisArgs.Concat(key, RedisArgs.GetTupleArgs(memberScores));
            return new RedisInt("ZADD", args);
        }
        public static RedisInt ZAdd(string key, params string[] memberScores)
        {
            object[] args = RedisArgs.Concat(key, memberScores);
            return new RedisInt("ZADD", args);
        }
        public static RedisInt ZCard(string key)
        {
            return new RedisInt("ZCARD", key);
        }
        public static RedisInt ZCount(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)
        {
            string min_score = RedisArgs.GetScore(min, exclusiveMin);
            string max_score = RedisArgs.GetScore(max, exclusiveMax);

            return new RedisInt("ZCOUNT", key, min_score, max_score);
        }
        public static RedisFloat ZIncrBy(string key, double increment, string member)
        {
            return new RedisFloat("ZINCRBY", key, increment, member);
        }
        public static RedisInt ZInterStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            List<object> args = new List<object>();
            args.Add(destination);
            args.Add(keys.Length);
            args.AddRange(keys);
            if (weights != null && weights.Length > 0)
            {
                args.Add("WEIGHTS");
                foreach (var weight in weights)
                    args.Add(weight);
            }
            if (aggregate != null)
            {
                args.Add("AGGREGATE");
                args.Add(aggregate.ToString().ToUpper());
            }
            return new RedisInt("ZINTERSTORE", args.ToArray());
        }
        public static RedisStrings ZRange(string key, long start, long stop, bool withScores = false)
        {
            string[] args = withScores
                ? new[] { key, start.ToString(), stop.ToString(), "WITHSCORES" }
                : new[] { key, start.ToString(), stop.ToString() };
            return new RedisStrings("ZRANGE", args);
        }
        public static RedisStrings ZRangeByScore(string key, double min, double max, bool withScores = false, bool exclusiveMin = false, bool exclusiveMax = false, long? offset = null, long? count = null)
        {
            string min_score = RedisArgs.GetScore(min, exclusiveMin);
            string max_score = RedisArgs.GetScore(max, exclusiveMax);

            string[] args = new[] { key, min_score, max_score };
            if (withScores)
                args = RedisArgs.Concat(args, new[] { "WITHSCORES" });
            if (offset.HasValue && count.HasValue)
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });

            return new RedisStrings("ZRANGEBYSCORE", args);
        }
        public static RedisIntNull ZRank(string key, string member) 
        {
            return new RedisIntNull("ZRANK", key, member);
        }
        public static RedisInt ZRem(string key, params string[] members) 
        {
            string[] args = RedisArgs.Concat(new[] { key }, members);
            return new RedisInt("ZREM", args);
        }
        public static RedisInt ZRemRangeByRank(string key, long start, long stop)
        {
            return new RedisInt("ZREMRANGEBYRANK", key, start, stop);
        }
        public static RedisInt ZRemRangeByScore(string key, double min, double max, bool exclusiveMin = false, bool exclusiveMax = false)  
        {
            string min_score = RedisArgs.GetScore(min, exclusiveMin);
            string max_score = RedisArgs.GetScore(max, exclusiveMax);

            return new RedisInt("ZREMRANGEBYSCORE", key, min_score, max_score);
        }
        public static RedisStrings ZRevRange(string key, long start, long stop, bool withScores = false)
        {
            string[] args = withScores
                ? new[] { key, start.ToString(), stop.ToString(), "WITHSCORES" }
                : new[] { key, start.ToString(), stop.ToString() };
            return new RedisStrings("ZREVRANGE", args);
        }
        public static RedisStrings ZRevRangeByScore(string key, double max, double min, bool withScores = false, bool exclusiveMax = false, bool exclusiveMin = false, long? offset = null, long? count = null)
        {
            string min_score = RedisArgs.GetScore(min, exclusiveMin);
            string max_score = RedisArgs.GetScore(max, exclusiveMax);

            string[] args = new[] { key, max_score, min_score };
            if (withScores)
                args = RedisArgs.Concat(args, new[] { "WITHSCORES" });
            if (offset.HasValue && count.HasValue)
                args = RedisArgs.Concat(args, new[] { "LIMIT", offset.Value.ToString(), count.Value.ToString() });

            return new RedisStrings("ZREVRANGEBYSCORE", args);
        }
        public static RedisIntNull ZRevRank(string key, string member)
        {
            return new RedisIntNull("ZREVRANK", key, member);
        }
        public static RedisFloatNull ZScore(string key, string member)
        {
            return new RedisFloatNull("ZSCORE", key, member);
        }
        public static RedisInt ZUnionStore(string destination, double[] weights = null, RedisAggregate? aggregate = null, params string[] keys)
        {
            List<object> args = new List<object>();
            args.Add(destination);
            args.Add(keys.Length);
            args.AddRange(keys);
            if (weights != null && weights.Length > 0)
            {
                args.Add("WEIGHTS");
                foreach (var weight in weights)
                    args.Add(weight);
            }
            if (aggregate != null)
            {
                args.Add("AGGREGATE");
                args.Add(aggregate.ToString().ToUpper());
            }
            return new RedisInt("ZUNIONSTORE", args.ToArray());
        }
        #endregion

        #region PubSub
        public static RedisSubscription PSubscribe(params string[] channelPatterns)
        {
            return new RedisSubscription("PSUBSCRIBE", channelPatterns);
        }
        public static RedisInt Publish(string channel, string message)
        {
            return new RedisInt("PUBLISH", channel, message);
        }
        public static RedisSubscription PUnsubscribe(params string[] channelPatterns)
        {
            return new RedisSubscription("PUNSUBSCRIBE", channelPatterns);
        }
        public static RedisSubscription Subscribe(params string[] channels)
        {
            return new RedisSubscription("SUBSCRIBE", channels);
        }
        public static RedisSubscription Unsubscribe(params string[] channels)
        {
            return new RedisSubscription("UNSUBSCRIBE", channels);
        }
        #endregion

        #region Scripting
        public static RedisObject Eval(string script, string[] keys, params string[] arguments)
        {
            string[] args = RedisArgs.Concat(new object[] { script, keys.Length }, keys, arguments);
            return new RedisObject("EVAL", args);
        }
        public static RedisObject EvalSHA(string sha1, string[] keys, params string[] arguments)
        {
            string[] args = RedisArgs.Concat(new object[] { sha1, keys.Length }, keys, arguments);
            return new RedisObject("EVALSHA", args);
        }
        public static RedisBools ScriptExists(params string[] scripts)
        {
            return new RedisBools("SCRIPT EXISTS", scripts);
        }
        public static RedisStatus ScriptFlush()
        {
            return new RedisStatus("SCRIPT FLUSH");
        }
        public static RedisStatus ScriptKill()
        {
            return new RedisStatus("SCRIPT KILL");
        }
        public static RedisString ScriptLoad(string script)
        {
            return new RedisString("SCRIPT LOAD", script);
        }
        #endregion

        #region Strings
        public static RedisInt Append(string key, object value)
        {
            return new RedisInt("APPEND", key, value);
        }
        public static RedisInt BitCount(string key, long? start = null, long? end = null)
        {
            string[] args = start.HasValue && end.HasValue
                ? new[] { key, start.Value.ToString(), end.Value.ToString() }
                : new[] { key };
            return new RedisInt("BITCOUNT", args);
        }
        public static RedisInt BitOp(RedisBitOp operation, string destKey, params string[] keys)
        {
            string[] args = RedisArgs.Concat(new[] { operation.ToString().ToUpper(), destKey }, keys);
            return new RedisInt("BITOP", args);
        }
        public static RedisInt Decr(string key)
        {
            return new RedisInt("DECR", key);
        }
        public static RedisInt DecrBy(string key, long decrement)
        {
            return new RedisInt("DECRBY", key, decrement);
        }
        public static RedisString Get(string key)
        {
            return new RedisString("GET", key);
        }
        public static RedisBool GetBit(string key, uint offset)
        {
            return new RedisBool("GETBIT", key, offset);
        }
        public static RedisString GetRange(string key, long start, long end) 
        {
            return new RedisString("GETRANGE", key, start, end);
        }
        public static RedisString GetSet(string key, object value)
        {
            return new RedisString("GETSET", key, value);
        }
        public static RedisInt Incr(string key)
        {
            return new RedisInt("INCR", key);
        }
        public static RedisInt IncrBy(string key, long increment)
        {
            return new RedisInt("INCRBY", key, increment);
        }
        public static RedisFloat IncrByFloat(string key, double increment)
        {
            return new RedisFloat("INCRBYFLOAT", key, increment);
        }
        public static RedisStrings MGet(params string[] keys)
        {
            return new RedisStrings("MGET", keys);
        }
        public static RedisStatus MSet(params Tuple<string, string>[] keyValues)
        {
            object[] args = RedisArgs.GetTupleArgs(keyValues);
            return new RedisStatus("MSET", args);
        }
        public static RedisStatus MSet(params string[] keyValues)
        {
            return new RedisStatus("MSET", keyValues);
        }
        public static RedisBool MSetNx(params Tuple<string, string>[] keyValues)
        {
            object[] args = RedisArgs.GetTupleArgs(keyValues);
            return new RedisBool("MSETNX", args);
        }
        public static RedisBool MSetNx(params string[] keyValues)
        {
            return new RedisBool("MSETNX", keyValues);
        }
        public static RedisStatus PSetEx(string key, long milliseconds, object value)
        {
            return new RedisStatus("PSETEX", key, milliseconds, value);
        }
        public static RedisStatus Set(string key, object value)
        {
            return new RedisStatus("SET", key, value);
        }
        public static RedisStatusNull Set(string key, object value, TimeSpan expiration, RedisExistence? condition = null) 
        {
            return Set(key, value, (long)expiration.TotalMilliseconds, condition);
        }
        public static RedisStatusNull Set(string key, object value, int? expirationSeconds = null, RedisExistence? condition = null)
        {
            return Set(key, value, expirationSeconds, null, condition);
        }
        public static RedisStatusNull Set(string key, object value, long? expirationMilliseconds = null, RedisExistence? condition = null)
        {
            return Set(key, value, null, expirationMilliseconds, condition);
        }
        private static RedisStatusNull Set(string key, object value, int? expirationSeconds = null, long? expirationMilliseconds = null, RedisExistence? exists = null)
        {
            var args = new List<string> { key, value.ToString() };
            if (expirationSeconds != null)
                args.AddRange(new[] { "EX", expirationSeconds.ToString() });
            if (expirationMilliseconds != null)
                args.AddRange(new[] { "PX", expirationMilliseconds.ToString() });
            if (exists != null)
                args.AddRange(new[] { exists.ToString().ToUpper() });
            return new RedisStatusNull("SET", args.ToArray());
        }
        public static RedisBool SetBit(string key, uint offset, bool value)
        {
            return new RedisBool("SETBIT", key, offset, value ? "1" : "0");
        }
        public static RedisStatus SetEx(string key, long seconds, object value)
        {
            return new RedisStatus("SETEX", key, seconds, value);
        }
        public static RedisBool SetNx(string key, object value)
        {
            return new RedisBool("SETNX", key, value);
        }
        public static RedisInt SetRange(string key, uint offset, object value)
        {
            return new RedisInt("SETRANGE", key, offset, value);
        }
        public static RedisInt StrLen(string key)
        {
            return new RedisInt("STRLEN", key);
        }
        #endregion

        #region Server
        public static RedisStatus BgRewriteAof()
        {
            return new RedisStatus("BGREWRITEAOF");
        }
        public static RedisStatus BgSave()
        {
            return new RedisStatus("BGSAVE");
        }
        public static RedisString ClientGetName()
        {
            return new RedisString("CLIENT GETNAME");
        }
        public static RedisStatus ClientKill(string ip, int port)
        {
            return new RedisStatus("CLIENT KILL", ip, port);
        }
        public static RedisString ClientList()
        {
            return new RedisString("CLIENT LIST");
        }
        public static RedisStatus ClientSetName(string connectionName)
        {
            return new RedisStatus("CLIENT SETNAME");
        }
        public static RedisString ConfigGet(string parameter)
        {
            return new RedisString("CONFIG GET", parameter);
        }
        public static RedisStatus ConfigResetStat()
        {
            return new RedisStatus("CONFIG RESETSTAT");
        }
        public static RedisStatus ConfigSet(string parameter, string value)
        {
            return new RedisStatus("CONFIG SET", parameter, value);
        }
        public static RedisInt DbSize()
        {
            return new RedisInt("DBSIZE");
        }
        public static RedisStatus DebugObject(string key)
        {
            return new RedisStatus("DEBUG OBJECT", key);
        }
        public static RedisStatus DebugSegFault()
        {
            return new RedisStatus("DEBUG SEGFAULT");
        }
        public static RedisStatus FlushAll()
        {
            return new RedisStatus("FLUSHALL");
        }
        public static RedisStatus FlushDb()
        {
            return new RedisStatus("FLUSHDB");
        }
        public static RedisString Info(string section = null)
        {
            return new RedisString("INFO", section == null ? new string[0] : new[] { section });
        }
        public static RedisDate LastSave()
        {
            return new RedisDate("LASTSAVE");
        }
        public static RedisStatus Monitor()
        {
            return new RedisStatus("MONITOR");
        }
        public static RedisStatus Save()
        {
            return new RedisStatus("SAVE");
        }
        public static RedisStatus Shutdown(bool? save = null)
        {
            string[] args;
            if (save.HasValue && save.Value)
                args = new[] { "SAVE" };
            else if (save.HasValue && !save.Value)
                args = new[] { "NOSAVE" };
            else
                args = new string[0];
            return new RedisStatus("SHUTDOWN", args);
        }
        public static RedisStatus SlaveOf(string host, int port)
        {
            return new RedisStatus("SLAVEOF", host, port);
        }
        public static RedisStatus SlaveOfNoOne()
        {
            return new RedisStatus("SLAVEOF", "NO", "ONE");
        }
        public static RedisObject SlowLog(RedisSlowLog subCommand, string argument = null)
        {
            if (argument == null)
                return new RedisObject("SLOWLOG", subCommand.ToString().ToUpper());
            else
                return new RedisObject("SLOWLOG", subCommand.ToString().ToUpper(), argument);
        }
        public static RedisBytes Sync()
        {
            return new RedisBytes("SYNC");
        }
        public static RedisDateMicro Time()
        {
            return new RedisDateMicro("TIME");
        }
        #endregion

        #region Transactions
        public static RedisStatus Discard()
        {
            return new RedisStatus("DISCARD");
        }
        public static RedisObjects Exec()
        {
            return new RedisObjects("EXEC");
        }
        public static RedisStatus Multi()
        {
            return new RedisStatus("MULTI");
        }
        public static RedisStatus Unwatch()
        {
            return new RedisStatus("UNWATCH");
        }
        public static RedisStatus Watch(params string[] keys)
        {
            return new RedisStatus("WATCH", keys);
        }
        #endregion

        #region Sentinel
        public static RedisHashes<RedisSentinelInfo> Sentinels(string masterName)
        {
            return new RedisHashes<RedisSentinelInfo>("SENTINEL", "sentinels", masterName);
        }
        public static RedisHashes<RedisMasterInfo> Masters()
        {
            return new RedisHashes<RedisMasterInfo>("SENTINEL", "masters");
        }
        public static RedisHashes<RedisSlaveInfo> Slaves(string masterName)
        {
            return new RedisHashes<RedisSlaveInfo>("SENTINEL", "slaves", masterName);
        }
        public static RedisStrings IsMasterDownByAddr(string ip, int port)
        {
            return new RedisStrings("SENTINEL", "is-master-down-by-addr", ip, port);
        }
        public static RedisTuple<string, int> GetMasterAddrByName(string masterName)
        {
            return new RedisTuple<string, int>("SENTINEL", "get-master-addr-by-name", masterName);
        }
        public static RedisStatus Reset(string pattern)
        {
            return new RedisStatus("SENTINEL", pattern);
        }
        #endregion
    }

    abstract class RedisCommand<T>
    {
        public Func<Stream, T> Parser { get; protected set; }
        public string Command { get; private set; }
        public object[] Arguments { get; private set; }

        protected RedisCommand(Func<Stream, T> parser, string command, params object[] args)
        {
            Parser = parser;
            Command = command;
            Arguments = args;
        }
    }
}
