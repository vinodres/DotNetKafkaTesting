using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading;
using Common.Logging;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace TestKafkaPublishService
{
    /// <summary>
    /// 
    /// </summary>
    public class TestKafkaProvider : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private Producer<string, string> _producer;

        private CancellationToken _cancellationToken;

        private readonly string _brokerUrl;

        private readonly string _clientId;

        private string ClientId => GetClientIdByHostMachine();

        private readonly Stopwatch _stopWatch = new Stopwatch();
        private readonly object _publishSyncObject = new object();

        /// <summary>
        /// Intialize provider using producer configuration details
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="brokerUrl"></param>
        /// <param name="clientId"></param>
        public TestKafkaProvider(CancellationToken cancellationToken, string brokerUrl, string clientId)
        {
            _cancellationToken = cancellationToken;
            _brokerUrl = brokerUrl;
            _clientId = clientId;
            InitializeProducer();
            _stopWatch.Start();
        }

        private void InitializeProducer()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Log.Info($"Cancellation requested, ignored producer initilization.");
                return;
            }


            var config = new Dictionary<string, object>()
            {
                {"bootstrap.servers", _brokerUrl},
                {"client.id", ClientId},
                {"debug", "protocol"},
                {"socket.blocking.max.ms", 5}, //must be always <= queue.buffering.max.ms
                {"queue.buffering.max.ms", 10},
                {
                    "default.topic.config", new Dictionary<string, object>()
                    {
                        {"acks", -1} // Client will get acknowledgement only after message is published and replicated to all replicas
                    }
                }
            };


            _producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));
            _producer.OnError += OnProducerError;
            _producer.OnLog += OnProducerLog;

            Log.Info($"Producer {_producer.Name} with Library version {Library.VersionString} is initilized.");
        }

        private void OnProducerLog(object sender, LogMessage e)
        {
            Log.Debug($"ConfluentKafka : [{e.Name}] [{e.Facility}] [{e.Level}] [{e.Message}]");
        }

        private void OnProducerError(object sender, Error e)
        {
            //e.
            Log.Error($"Publisher error occured with code [{e.Code}], HasError [{e.HasError}] IsBrokerError [{e.IsBrokerError}], IsLocalError [{e.IsLocalError}] reason [{e.Reason}].");
        }


        private string GetClientIdByHostMachine()
        {
            return $"{_clientId}.{Environment.MachineName}";
        }

        /// <summary>
        /// 
        /// </summary>
        public void Publish(string message, string topic)
        {
            lock (_publishSyncObject)
            {

                if (_cancellationToken.IsCancellationRequested)
                {
                    Log.Info($"Cancellation requested, publishing message ignored.");
                    return;
                }

               _stopWatch.Restart();

                var producerMessageTask = _producer.ProduceAsync(topic, "", message);
                var result = producerMessageTask.Result;

                Log.Trace($"Message [{message}] publish took [{_stopWatch.ElapsedMilliseconds}] milli sec");

            }
        }

        /// <summary>
        /// releases all resources held by the producer
        /// </summary>
        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}