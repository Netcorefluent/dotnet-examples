using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace DataGenerator
{
    class Program
    {
        private static readonly string[] _tribute = { "this", "is", "not", "the", "greatest", "song", "in", "the", "world", "this", "is", "just", "a", "tribute" };

        private static readonly Random rand = new Random();

        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            // A Producer for sending messages with UTF-8 encoded keys and UTF-8 encoded values.
            using (var p = new Producer<string, string>(config))
            {
                try
                {
                    while (true)
                    {
                        var dr = await p.ProduceAsync("words", new Message<string, string>
                        {
                            Key = rand.Next(5).ToString(),
                            Value = _tribute[rand.Next(_tribute.Length)]
                        });
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                        Thread.Sleep(100);
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
