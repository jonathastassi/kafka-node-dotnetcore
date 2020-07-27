using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Log.Service
{
    class Program
    {
        static void Main(string[] args)
        {
          var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "ECOMMERCE_LOG_SERVICE",

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
                consumer.Subscribe("^ECOMMERCE*");

                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);

                    Console.WriteLine("LOG");
                    Console.WriteLine(consumeResult.Topic);
                    // handle consumed message.

                }

            consumer.Close();
            }
        }
    }
}
