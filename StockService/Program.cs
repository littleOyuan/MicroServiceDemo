using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using RedisOpreator;

namespace StockService
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                HostName = "10.10.21.214",
                UserName = "admin",
                Password = "colipu",
                VirtualHost = "/"
            };

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("orderExchange", ExchangeType.Fanout, true);

                    string queueName = channel.QueueDeclare("StockQueue", true, false, false, null).QueueName;

                    channel.QueueBind(queueName, "orderExchange", "");

                    Console.WriteLine(" [StockService] Waiting for logs.");

                    EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(channel);

                    eventingBasicConsumer.Received += (model, eventArgs) =>
                    {
                        byte[] body = eventArgs.Body;

                        string message = Encoding.UTF8.GetString(body);

                        Console.WriteLine(" [StockService] Received {0} ------------> 库存减少 1 ", message);

                        channel.BasicAck(eventArgs.DeliveryTag, false);

                        RedisHelper.SetRemove("OrderId-123456", "2");
                    };

                    channel.BasicConsume(queueName, false, eventingBasicConsumer);

                    Console.WriteLine(" Press [enter] to exit.");

                    Console.ReadLine();
                }
            }
        }
    }
}
