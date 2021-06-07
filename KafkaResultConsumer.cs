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
            //Task.Run(() =>
            //{
            //    using var consumer = new ConsumerBuilder<int, byte[]>(_kafkaConfig)
            //        .Build();
            //    consumer.Subscribe("messageResult");
            //    var consumeResult = consumer.Consume();
            //    var deserializedMessage = MessagePackSerializer.Deserialize<MessageModel>(consumeResult.Message.Value);
            //    Console.WriteLine("Message Consumed from Kafka: {0} Length: {1}", deserializedMessage.Message, deserializedMessage.MessageLength);
            //    _result.Enqueue(deserializedMessage);
            //});

            Task.Run(() =>
            {
                using var c = new ConsumerBuilder<int, byte[]>(_kafkaConfig).Build();
                c.Subscribe("messageResult");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            var deserializedMessage = MessagePackSerializer.Deserialize<MessageModel>(cr.Message.Value);
                            Console.WriteLine($"Consumed from Kafka message '{deserializedMessage.Message}' with length {deserializedMessage.MessageLength}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occurred: {e.Error.Reason}");

                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            });
        }
    }
}