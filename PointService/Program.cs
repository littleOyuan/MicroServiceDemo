using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using RedisOpreator;

namespace PointService
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

                    string queueName = channel.QueueDeclare("PointQueue", true, false, false, null).QueueName;

                    channel.QueueBind(queueName, "orderExchange", "");

                    Console.WriteLine(" [PointService] Waiting for logs.");

                    EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(channel);

                    eventingBasicConsumer.Received += (model, evnetArgs) =>
                    {
                        byte[] body = evnetArgs.Body;

                        string message = Encoding.UTF8.GetString(body);

                        Console.WriteLine(" [PointService] Received {0} ------------> 积分增加 1 ", message);

                        channel.BasicAck(evnetArgs.DeliveryTag, false);

                        RedisHelper.SetRemove("OrderId-123456", "1");
                    };

                    channel.BasicConsume(queueName, false, eventingBasicConsumer);

                    Console.WriteLine(" Press [enter] to exit.");

                    Console.ReadLine();
                }
            }
        }
    }
}
