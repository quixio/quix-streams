using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Kafka;
using QuixStreams.Kafka.Transport;
using QuixStreams.Telemetry.Configuration;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Speedtest
{
    public class TransportTest
    {
        
        
        public void Run(CancellationToken ct)
        {
            CodecRegistry.Register(CodecType.Protobuf);

            var times = new List<double>();
            var timesTotal = 0;
            var timesLock = new object();
            
            byte magicMarker = 17;
            
            var configBuilder = new SecurityOptionsBuilder();
            if (Configuration.Config.Security != null)
            {
                configBuilder.SetSaslAuthentication(Configuration.Config.Security.Username,
                    Configuration.Config.Security.Password, SaslMechanism.ScramSha256);
                configBuilder.SetSslEncryption(Configuration.Config.Security.SslCertificates);
            }

            var config = configBuilder.Build();
            
            if (!string.IsNullOrWhiteSpace(Configuration.Config.ConsumerId))
            {
                config["group.id"] = Configuration.Config.ConsumerId;
            }

            IKafkaProducer CreateProducer()
            {
                var prodConfig = new ProducerConfiguration(Configuration.Config.BrokerList, config);
                var topicConfig = new ProducerTopicConfiguration(Configuration.Config.Topic);
                var kafkaProducer = new KafkaProducer(prodConfig, topicConfig);
                return kafkaProducer;
            }

            var start = DateTime.UtcNow;
            var lastpackageRead = DateTime.UtcNow;

            var consConfig = new ConsumerConfiguration(Configuration.Config.BrokerList, "Debug", config);
            var topicConfig = new ConsumerTopicConfiguration(Configuration.Config.Topic);
            var kafkaOutput = new KafkaConsumer(consConfig, topicConfig);
            kafkaOutput.OnErrorOccurred += (s, e) => { Console.WriteLine($"Exception occurred: {e}"); };
            kafkaOutput.Open();
            var consumer = new KafkaTransportConsumer(kafkaOutput);
            consumer.OnPackageReceived = (package) =>
            {
                lastpackageRead = DateTime.UtcNow;
                var now = DateTime.UtcNow;
                if (!package.TryConvertTo<byte[]>(out var converted) || converted.Value.Length != 9 || converted.Value[0] != magicMarker)
                {
                    Console.WriteLine("Ignoring package, PartitionOffset: " + package.KafkaMessage.TopicPartitionOffset);
                    return Task.CompletedTask; // not our package 
                };

                var binaryTime = BitConverter.ToInt64(converted.Value, 1);
                var sentAt = DateTime.FromBinary(binaryTime);
                if (start > sentAt) return Task.CompletedTask; // possible previous run
                //Console.WriteLine($"Sent: {sentAt:O}");
                var elapsed = (now - sentAt).TotalMilliseconds;
                //Console.WriteLine($"    Arrived: (+{elapsed}) {now:O}");
                lock (timesLock)
                {
                    times.Add(elapsed);
                    timesTotal++;
                    times = times.Skip(Math.Min(0,times.Count-50)).ToList();

                    Console.WriteLine("Avg: " + Math.Round(times.Average(), 2) + ", Max: " + Math.Round(times.Max(), 2) + ", Min: " + Math.Round(times.Min(), 2) + ", over last " + times.Count + " out of " + timesTotal);
                }

                return Task.CompletedTask;
            };

            while (DateTime.UtcNow < lastpackageRead.AddSeconds(2))
            {
                // get to end...
                Thread.Sleep(100);
            }
            using (var kafkaProducer = CreateProducer())
            {
                var transportProducer = new KafkaTransportProducer(kafkaProducer);
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var bytes = new byte[9];
                        bytes[0] = magicMarker; //just a marker
                        var binary = DateTime.UtcNow.ToBinary();
                        Array.Copy(BitConverter.GetBytes(binary), 0, bytes, 1, 8);
                        var msg = new TransportPackage<byte[]>("someKey", bytes, null);
                        transportProducer.Publish(msg, ct);
                        Thread.Sleep(100);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
            }
        }
    }
}