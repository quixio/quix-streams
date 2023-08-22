using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;
using QuixStreams.Streaming;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Models.StreamProducer;
using QuixStreams.Streaming.Utils;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Tester
{
    class Program
    {
        private static CancellationTokenSource cts = new CancellationTokenSource();

        private static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };
            
            Logging.UpdateFactory(LogLevel.Debug);
            
            
            CodecSettings.SetGlobalCodecType(CodecType.Json);
            PackageSerializationSettings.Mode = PackageSerializationMode.Header; // required for new mode
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);


            if (Configuration.Mode == ClientRunMode.Producer)
            {
                Produce(client, cts.Token);
            }

            Consume(client, cts.Token);
        }

        private static void Produce(KafkaStreamingClient client, CancellationToken cancellationToken)
        {
            using var topicProducer = client.GetTopicProducer(Configuration.Config.Topic);

            using var stream = topicProducer.CreateStream();

            if (Configuration.ProducerConfig.Timeseries)
            {
                Task.Run(() =>
                {
                    stream.Timeseries.Buffer.TimeSpanInMilliseconds = 1000;
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var tsdb = stream.Timeseries.Buffer.AddTimestamp(DateTime.UtcNow);

                        var random = new Random();
                        for (var ii = 1; ii <= 10; ii++)
                        {
                            if (random.Next(0, 3) == 1) tsdb.AddValue($"numeric_parameter_{ii}", ii);
                            if (random.Next(0, 3) == 1) tsdb.AddValue($"string_parameter_{ii}", $"value_{ii}");
                            if (random.Next(0, 3) == 1)
                                tsdb.AddValue($"binary_parameter_{ii}", Encoding.UTF8.GetBytes($"binary_value_{ii}"));
                        }

                        if (random.Next(0, 2) == 1) tsdb.AddTag("Random_Tag", $"tag{random.Next(0, 10)}");

                        tsdb.Publish();
                        
                        Thread.Sleep(250);
                    }
                });
            }
            
            if (Configuration.ProducerConfig.Event)
            {
                Task.Run(() =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {

                        var random = new Random();
                        var obj = new
                        {
                            PropOne = "Value " + random.Next(10, 99999),
                            PropTwo = random.NextDouble() * 10000,
                            PropThree = random.Next(0, 2) == 1
                        };
                        
                        var builder = stream.Events.AddTimestamp(DateTime.UtcNow);
                            
                        builder.AddValue("an_event", Newtonsoft.Json.JsonConvert.SerializeObject(obj));

                        if (random.Next(0, 2) == 1) builder.AddTag("Random_Tag", $"tag{random.Next(0, 10)}");

                        builder.Publish();
                        stream.Events.Flush();
                        
                        Thread.Sleep(1000);
                    }
                });
            }

            try
            {
                cancellationToken.WaitHandle.WaitOne();
            }
            catch
            {
                
            }
        }
        
        private static void Consume(KafkaStreamingClient client, CancellationToken cancellationToken)
        {
            using var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerGroup, CommitMode.Automatic, AutoOffsetReset.Earliest);

            topicConsumer.OnStreamReceived += (sender, consumer) =>
            {
                Console.WriteLine($"Received new stream {consumer.StreamId}");
                consumer.Timeseries.OnRawReceived += (o, args) =>
                {
                    Console.WriteLine($"Received new timeseries data for {consumer.StreamId}");
                    var asJson = Newtonsoft.Json.JsonConvert.SerializeObject(args.Data, Formatting.Indented);
                    Console.WriteLine(asJson);
                };

                consumer.Events.OnDataReceived += (o, args) =>
                {
                    Console.WriteLine($"Received new event data for {consumer.StreamId}");
                    var asJson = Newtonsoft.Json.JsonConvert.SerializeObject(args.Data, Formatting.Indented);
                    Console.WriteLine(asJson);
                };
            };
            
            topicConsumer.Subscribe();

            
            try
            {
                cancellationToken.WaitHandle.WaitOne();
            }
            catch
            {
                
            }
        }
    }
}


