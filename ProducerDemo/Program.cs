using System;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ProducerDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var logger = serviceProvider.GetService<ILogger<Program>>();

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            using var producer = new ProducerBuilder<Null, string>(config)
                .SetErrorHandler((producer, error) =>
                {
                    logger.LogError($"--Kafka producer Error: {error.Reason}");
                })
                .Build();
            for (int i = 0; i < 10; i++)
            {
                logger.LogInformation($"Message Id: {i}");
                var msg = new Message<Null, string>() { Value = $" {i} Hello World! [{DateTime.Now.ToString("HH:mm:ss_FFF")}]" };

                var task = producer.ProduceAsync("first-topic", msg);
                await task.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        logger.LogError($"--Kafka Producer Error");
                    }
                    else
                    {
                        logger.LogInformation($"Received new metadata. \n" +
                            $"Topic: {t.Result.Topic}\n" +
                            $"Partition: {t.Result.Partition}\n" +
                            $"Wrote to offset: {t.Result.Offset}\n" +
                            $"Timestamp: {t.Result.Timestamp}\n");
                    }
                });

            }

            logger.LogInformation("Producer Flush");
            producer.Flush(TimeSpan.FromSeconds(1));
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(configure => configure.AddConsole());
        }
    }
}
