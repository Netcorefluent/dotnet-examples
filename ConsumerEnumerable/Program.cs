using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace ConsumerEnumerable
{
    class Program
    {
        static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-words-consumer",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var kafkaConsumer = new KafkaConsumerEnumerable<string, string>(conf))
            {
                kafkaConsumer
                .Where(x => x.Key == "4")
                .Take(500)
                .ToList() 
                .ForEach(m => Console.WriteLine($"Consumed message '{m.Value}' at: '{m.TopicPartitionOffset}'."));
            }
        }

        public class KafkaConsumerEnumerable<TKey, TValue> : IEnumerable<ConsumeResult<TKey, TValue>>, IDisposable
        {

            private readonly Consumer<TKey, TValue> _consumer;

            private bool disposedValue = false;
            public KafkaConsumerEnumerable(ConsumerConfig kafkaConfiguration)
            {
                _consumer = new Consumer<TKey, TValue>(kafkaConfiguration);
                _consumer.Subscribe("words");

            }
            public IEnumerator<ConsumeResult<TKey, TValue>> GetEnumerator()
            {
                for (; ; )
                {
                    ConsumeResult<TKey, TValue> ret = null;
                    try
                    {
                        ret = _consumer.Consume();
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                        throw e;
                    }
                    yield return ret;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Dispose()
            {
                if (!disposedValue)
                {
                    _consumer.Close();
                    _consumer.Dispose();
                    disposedValue = true;
                }
            }
        }
    }
}
