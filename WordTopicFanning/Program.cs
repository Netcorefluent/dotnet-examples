using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace WordTopicFanning
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            var conf = new ConsumerConfig
            {
                GroupId = "test-words-consumer",
                BootstrapServers = "localhost:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // eariest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var c = new Consumer<string, string>(conf))
            {
                c.Subscribe("words");

                bool consuming = true;
                // The client will automatically recover from non-fatal errors. You typically
                // don't need to take any action unless an error is marked as fatal.
                c.OnError += (_, e) => consuming = !e.IsFatal;

                while (consuming)
                {
                    try
                    {
                        var cr = c.Consume();
                        using (var p = new Producer<string, string>(config))
                        {
                            try
                            {
                                var dr = await p.ProduceAsync($"words_{cr.Value}", new Message<string, string>
                                {
                                    Key = cr.Key,
                                    Value = cr.Value
                                });
                                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");

                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                            }
                        }
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }

                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }
}
