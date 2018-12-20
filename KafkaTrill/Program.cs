using System;
using System.Collections;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Linq;
using Confluent.Kafka;
using Microsoft.StreamProcessing;
using System.Threading.Tasks;

namespace KafkaTrill
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var conf = new ConsumerConfig
            {
                GroupId = "test-words-consumer-1",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var kafkaConsumer = new KafkaConsumerEnumerable<string, string>(conf))
            {
                await kafkaConsumer
                .Select(ConsumeResultToStreamEvent)
                .ToObservable()
                .ToStreamable()    
                .Where(cr => cr.Key == "4")
                .ToStreamEventObservable()
                .ForEachAsync(se => Console.WriteLine($"Consumed message '{se.Payload.Message}' at: '{se.Payload.TopicPartitionOffset}'."));
            }
        }

        private static StreamEvent<ConsumeResult<string,  string>> ConsumeResultToStreamEvent(ConsumeResult<string, string> consumeResult) {
            Console.WriteLine($"Consumed message '{consumeResult.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
            return StreamEvent.CreatePoint(consumeResult.Offset, consumeResult);
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
