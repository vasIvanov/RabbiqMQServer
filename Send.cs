﻿using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbiqMQServer
{
    class Send
    {
        public static void Main()
        {
            //var factory = new ConnectionFactory() { HostName = "localhost" };
            //using (var connection = factory.CreateConnection())
            //using (var channel = connection.CreateModel())
            //{
            //    channel.QueueDeclare(queue: "hello", durable: false, exclusive: false, autoDelete: false, arguments: null);

            //    string message = "Hello World!";
            //    var body = Encoding.UTF8.GetBytes(message);

            //    channel.BasicPublish(exchange: "", routingKey: "hello", basicProperties: null, body: body);
            //    Console.WriteLine(" [x] Sent {0}", message);
            //}

            //Console.WriteLine(" Press [enter] to exit.");
            //Console.ReadLine();

            var rabbit = new Producer();
            var kafka = new KafkaResultConsumer();
            kafka.ConsumeFromKafka();

            rabbit.Publish("msg");

            Console.ReadKey();
        }
    }
}
