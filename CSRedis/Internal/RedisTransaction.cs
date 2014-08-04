using CSRedis.Internal.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSRedis.Internal
{
    class RedisTransaction
    {
        readonly RedisConnection _connection;
        readonly RedisArray _execCommand;
        readonly List<Tuple<string, object[]>> _pipeCommands = new List<Tuple<string, object[]>>();

        public event EventHandler<RedisTransactionQueuedEventArgs> TransactionQueued;

        bool _active;
        public bool Active { get { return _active; } }

        public RedisTransaction(RedisConnection connection)
        {
            _connection = connection;
            _execCommand = RedisCommand.Exec();
        }

        public string Start()
        {
            _active = true;
            return _connection.Call(RedisCommand.Multi());
        }

        public Task<string> StartAsync()
        {
            _active = true;
            return _connection.CallAsync(RedisCommand.Multi());
        }

        public T Write<T>(RedisCommand<T> command)
        {
            string response = _connection.Call(RedisCommand.AsTransaction(command));
            OnTransactionQueued(command, response);

            _execCommand.AddParser(x => command.Parse(x));
            return default(T);
        }

        public Task<T> WriteAsync<T>(RedisCommand<T> command)
        {
            lock (_execCommand)
            {
                _execCommand.AddParser(x => command.Parse(x));
                return _connection.CallAsync(RedisCommand.AsTransaction(command))
                    .ContinueWith(t => OnTransactionQueued(command, t.Result))
                    .ContinueWith(t => default(T));
            }
        }

        public object[] Execute()
        {
            _active = false;

            if (_connection.Connected && _connection.Pipelined)
            {
                _connection.Call(_execCommand);
                object[] response = _connection.EndPipe();
                for (int i = 0; i < response.Length - 1; i++)
                    OnTransactionQueued(_pipeCommands[i].Item1, _pipeCommands[i].Item2, response[i].ToString());
                
                object transaction_response = response[response.Length - 1];
                if (!(transaction_response is object[]))
                    throw new RedisProtocolException("Unexpected response");

                return transaction_response as object[];
            }

            return _connection.Call(_execCommand);
        }

        public Task<object[]> ExecuteAsync()
        {
            _active = false;
            return _connection.CallAsync(_execCommand);
        }

        public string Abort()
        {
            _active = false;
            return _connection.Call(RedisCommand.Discard());
        }

        public Task<string> AbortAsync()
        {
            _active = false;
            return _connection.CallAsync(RedisCommand.Discard());
        }

        void OnTransactionQueued<T>(RedisCommand<T> command, string response)
        {
            if (_connection.Pipelined)
                _pipeCommands.Add(Tuple.Create(command.Command, command.Arguments));
            else
                OnTransactionQueued(command.Command, command.Arguments, response);
        }

        void OnTransactionQueued(string command, object[] args, string response)
        {
            if (TransactionQueued != null)
                TransactionQueued(this, new RedisTransactionQueuedEventArgs(response, command, args));
        }
    }
}
