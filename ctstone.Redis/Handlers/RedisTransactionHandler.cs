using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using ctstone.Redis.IO;

namespace ctstone.Redis.Handlers
{
    class RedisTransactionHandler
    {
        private readonly RedisConnection _connection;
        private Queue<Delegate> _parsers;
        private ActivityTracer _activity;
        private bool _captureResults;

        public event EventHandler<RedisTransactionQueuedEventArgs> TransactionQueued;
        public event EventHandler<RedisTransactionStartedEventArgs> TransactionStarted;
        public bool Active { get; private set; }

        public RedisTransactionHandler(RedisConnection connection)
        {
            _connection = connection;
            _parsers = new Queue<Delegate>();
        }

        public void Start(bool captureResults = true)
        {
            _activity = new ActivityTracer("Begin transaction");
            Active = true;
            _captureResults = captureResults;
            string status = _connection.Call(RedisReader.ReadStatus, "MULTI");
            OnTransactionStarted(status);
        }
        public Task StartAsync(bool captureResults = true)
        {
            _activity = new ActivityTracer("Begin transaction");
            Active = true;
            _captureResults = captureResults;
            return _connection.CallAsync(RedisReader.ReadStatus, "MULTI")
                .ContinueWith(x => OnTransactionStarted(x.Result));
        }

        public string Discard()
        {
            Active = false;
            _activity.Dispose();
            return _connection.Call(RedisReader.ReadStatus, "DISCARD");
        }
        public Task<string> DiscardAsync()
        {
            Active = false;
            _activity.Dispose();
            return _connection.CallAsync(RedisReader.ReadStatus, "DISCARD");
        }

        public object[] Exec()
        {
            RedisObjects command = RedisCommand.Exec();
            Active = false;
            _activity.Dispose();
            return _connection.Call(ReadTransaction, command.Command, command.Arguments);
        }
        public Task<object[]> ExecAsync()
        {
            RedisObjects cmd = RedisCommand.Exec();
            Active = false;
            _activity.Dispose();
            return _connection.CallAsync(ReadTransaction, cmd.Command, cmd.Arguments);
        }

        public void Write<T>(RedisCommand<T> command)
        {
            if (!Active)
                throw new InvalidOperationException("Transaction not started before invoking transaction");

            EnqueueParser(command.Parser);
            string status = _connection.Call(RedisReader.ReadStatus, command.Command, command.Arguments);
            OnTransactionQueued(status, command.Command, command.Arguments);
        }

        public Task WriteAsync<T>(RedisCommand<T> command)
        {
            if (!Active)
                throw new InvalidOperationException("Transaction not started before invoking transaction");

            EnqueueParser(command.Parser);
            return _connection.CallAsync(RedisReader.ReadStatus, command.Command, command.Arguments)
                .ContinueWith(x => OnTransactionQueued(x.Result, command.Command, command.Arguments));
        }

        void OnTransactionQueued(string status, string command, object[] arguments)
        {
            if (TransactionQueued != null)
                TransactionQueued(this, new RedisTransactionQueuedEventArgs(status, command, arguments));
        }

        void OnTransactionStarted(string status)
        {
            if (TransactionStarted != null)
                TransactionStarted(this, new RedisTransactionStartedEventArgs(status));
        }

        void EnqueueParser(Delegate parser)
        {
            if (_captureResults)
                _parsers.Enqueue(parser);
        }

        object[] ReadTransaction(Stream input)
        {
            if (!_captureResults)
            {
                RedisObjects command = RedisCommand.Exec();
                command.Parser(input);
                return null;
            }

            RedisReader.ExpectType(input, RedisMessage.MultiBulk);
            long count = RedisReader.ReadInt(input, false);
            if (count == -1)
                return null;

            object[] output = new object[_parsers.Count];
            for (int i = 0; i < output.Length; i++)
                output[i] = _connection.Read(_parsers.Dequeue());
            return output;
        }
    }
}
