using ctstone.Redis.RedisCommands;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ctstone.Redis
{
    class RedisSubscriptionHandler
    {
        private readonly RedisConnection _connection;

        public bool IsSubscribed { get; private set; }
        public long Count { get; private set; }

        /// <summary>
        /// Occurs when a subscription message has been received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subsciption channel is opened or closed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        public RedisSubscriptionHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public void HandleSubscription(RedisSubscription command)
        {
            _connection.Write(command.Command, command.Arguments);
            if (!IsSubscribed)
            {
                using (new ActivityTracer("Handle subscriptions"))
                {
                    IsSubscribed = true;
                    while (true)
                    {
                        var resp = _connection.Read(command.Parser);
                        switch (resp.Type)
                        {
                            case RedisSubscriptionResponseType.Subscribe:
                            case RedisSubscriptionResponseType.PSubscribe:
                            case RedisSubscriptionResponseType.Unsubscribe:
                            case RedisSubscriptionResponseType.PUnsubscribe:
                                RedisSubscriptionChannel channel = resp as RedisSubscriptionChannel;
                                Count = channel.Count;
                                if (SubscriptionChanged != null)
                                    SubscriptionChanged(this, new RedisSubscriptionChangedEventArgs(channel));
                                break;

                            case RedisSubscriptionResponseType.Message:
                            case RedisSubscriptionResponseType.PMessage:
                                RedisSubscriptionMessage message = resp as RedisSubscriptionMessage;
                                if (SubscriptionReceived != null)
                                    SubscriptionReceived(this, new RedisSubscriptionReceivedEventArgs(message));
                                break;
                        }
                        if (Count == 0)
                            break;
                    }
                    IsSubscribed = false;
                }
            }
        }
    }

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

    class RedisMonitorHandler
    {
        private readonly RedisConnection _connection;

        /// <summary>
        /// Occurs when a monitor response is received
        /// </summary>
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        public RedisMonitorHandler(RedisConnection connection)
        {
            _connection = connection;
        }

        public string Monitor()
        {
            using (new ActivityTracer("Beging monitor"))
            {
                string status = _connection.Call(RedisReader.ReadStatus, "MONITOR");
                while (true)
                {
                    object message;
                    try
                    {
                        message = _connection.Read();
                    }
                    catch (Exception e)
                    {
                        if (_connection.Connected) throw e;
                        return status;
                    }
                    if (MonitorReceived != null)
                        MonitorReceived(this, new RedisMonitorEventArgs(message));
                }
            }
        }
    }
}
