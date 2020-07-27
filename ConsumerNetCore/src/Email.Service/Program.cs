using System;
using System.Threading;
using Confluent.Kafka;

namespace Email.Service
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "ECOMMERCE_EMAIL_SERVICE",

                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("ECOMMERCE_SEND_EMAIL");

                 while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);

                    Console.WriteLine(consumeResult.Message.Value);
                    // handle consumed message.

                }

                consumer.Close();
            }
        }
    }
}

