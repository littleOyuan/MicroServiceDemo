using RabbitMQ.Client;
using RedisOpreator;
using System;
using System.Text;

namespace OrderService
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
                    string message = "This is a New Order...";

                    RedisHelper.KeyEventNotification(x => { Console.WriteLine("{0} is deleted!", x); });

                    RedisHelper.SetAdd("OrderId-123456", new[] { "1", "2" });

                    channel.ConfirmSelect();

                    channel.ExchangeDeclare("orderExchange", ExchangeType.Fanout, true);

                    channel.BasicPublish("orderExchange", "", null, Encoding.UTF8.GetBytes(message));

                    if (channel.WaitForConfirms())
                    {
                        Console.WriteLine(" [OrderService] Sent {0}", message);
                    }
                    else
                    {
                        Console.WriteLine(" [OrderService] Sent failed");
                    }
                }
            }

            Console.WriteLine(" Press [enter] to exit.");

            Console.ReadLine();
        }
    }
}
