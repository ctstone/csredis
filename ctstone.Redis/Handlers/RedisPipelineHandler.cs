using ctstone.Redis.Debug;
using ctstone.Redis.Commands;
using ctstone.Redis.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ctstone.Redis.Handlers
{
    class RedisPipelineHandler
    {
        private readonly Queue<object> Results;
        private readonly RedisConnection _connection;
        private readonly RedisTransactionHandler _transactionhandler;
        private ActivityTracer _activity;
        private Task _io;
        private bool _captureResults;
        private bool _asTransaction;

        public bool Active { get; private set; }

        public RedisPipelineHandler(RedisConnection connection, RedisTransactionHandler transactionHandler)
        {
            _connection = connection;
            _transactionhandler = transactionHandler;
            Results = new Queue<object>();
        }

        public void Start(bool captureResults = true)
        {
            _asTransaction = false;
            _activity = new ActivityTracer("Begin pipeline");
            _captureResults = captureResults;
            if (Active)
                throw new InvalidOperationException("Already in pipeline mode");
            Active = true;
        }

        public object[] End()
        {
            if (!Active)
                throw new InvalidOperationException("Not in pipeline mode");

            if (_asTransaction && _transactionhandler.Active)
                ExecTransaction();

            _io.Wait();
            Active = false;
            _activity.Dispose();

            object[] output = new object[Results.Count];
            for (int i = 0; i < output.Length; i++)
                output[i] = Results.Dequeue();
            return output;
        }

        // TODO: check for mismatch b/t _asTransaction and _transactionHandler.Active
        public void Write<T>(RedisCommand<T> command)
        {
            if (_transactionhandler.Active)
                _io = _transactionhandler.WriteAsync(command);
            else
                _io = _connection.CallAsync(command.Parser, command.Command, command.Arguments)
                    .ContinueWith(HandleResult);
        }

        public void StartTransaction(bool asTransaction = false)
        {
            _asTransaction = asTransaction;
            _io = _transactionhandler.StartAsync(_captureResults);
        }
        public void DiscardTransaction()
        {
            _asTransaction = false;
            _io = _transactionhandler.DiscardAsync()
                .ContinueWith(HandleResult);
        }
        public void ExecTransaction()
        {
            _io = _transactionhandler.ExecAsync()
                .ContinueWith(HandleResult);
        }

        private void HandleResult<T>(Task<T> completedTask)
        {
            if (!_captureResults)
                return;

            if (_asTransaction)
            {
                object[] transaction = completedTask.Result as object[];
                if (transaction != null)
                {
                    for (int i = 0; i < transaction.Length; i++)
                        Results.Enqueue(transaction[i]);
                }
            }
            else
                Results.Enqueue(completedTask.Result);
        }
    }
}
