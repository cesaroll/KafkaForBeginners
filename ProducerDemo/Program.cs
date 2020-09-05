using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace ProducerDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();
            for (int i = 0; i < 10; i++)
            {
                await producer.ProduceAsync("first-topic", new Message<Null, string>() { Value = $"Hello World! [{DateTime.Now.ToString("HH:mm:ss_FFF")}]" });

            }

            producer.Flush();
        }
    }
}
