using MessagePack;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbiqMQServer
{
    public class Producer : IDisposable
    {
        private IConnection _connection;
        private IModel _model;

        public Producer()
        {

        }
        public void Dispose()
        {
            _connection.Dispose();
            _model.Dispose();
        }

        public void Publish(string message)
        {
            Init();
            var msgObj = new MessageModel(message, 0);
            var serialize = MessagePackSerializer.Serialize(msgObj);
            _model.ExchangeDeclare("RabbitMQServer", ExchangeType.Fanout, false, false);
            _model.QueueDeclare("task_queue", true, false);

            _model.BasicPublish("", "task_queue", basicProperties: null, body: serialize);

            Console.WriteLine(" [x] Sent {0}", message);
        }

        public void Init()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            _connection = factory.CreateConnection();
            _model = _connection.CreateModel();
        }
    }
}
