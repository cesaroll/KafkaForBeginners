using System;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ConsumerDemo
{
    public class ConsumerDemo
    {
        static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var logger = serviceProvider.GetService<ILogger<ConsumerDemo>>();

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9092",
                GroupId = "my-fourth-application",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            //Create consumer
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            {
                // Subscribe consumer to our topic(s)
                consumer.Subscribe("first-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    // Poll new data
                    while (true)
                    {
                        var result = consumer.Consume(cts.Token);
                        if (result != null)
                        {
                            logger.LogInformation($"Key: {result.Message.Key}, Value: {result.Message.Value}");
                            logger.LogInformation($"Partition: {result.Partition}, Offset: {result.Offset}");
                        }

                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }


            }




        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(configure => configure.AddConsole());
        }

    }
}
