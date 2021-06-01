using Confluent.Kafka;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbiqMQServer
{
    public class KafkaResultConsumer
    {
        private readonly ConsumerConfig _kafkaConfig;
        public static ConcurrentQueue<MessageModel> _result = new ConcurrentQueue<MessageModel>();

        public KafkaResultConsumer()
        {
            _kafkaConfig = new ConsumerConfig
            {
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 5000,
                FetchWaitMaxMs = 50,
                BootstrapServers = "localhost:9092",
                GroupId = $"MessageServer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public void ConsumeFromKafka()
        {
            Task.Run(() =>
            {
                using var consumer = new ConsumerBuilder<int, byte[]>(_kafkaConfig)
                    .Build();
                consumer.Subscribe("messageResult");
                var consumeResult = consumer.Consume();
                var deserializedMessage = MessagePackSerializer.Deserialize<MessageModel>(consumeResult.Message.Value);
                _result.Enqueue(deserializedMessage);
                Console.WriteLine("Message: {0} Length: {1}", deserializedMessage.Message, deserializedMessage.MessageLength);
            });

      
        }
    }
}
