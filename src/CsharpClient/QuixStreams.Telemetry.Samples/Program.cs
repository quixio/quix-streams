﻿using System;
 using System.Threading;
 using QuixStreams.Kafka.Transport.SerDes;
 using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;
 using QuixStreams.Telemetry.Kafka;
 using QuixStreams.Telemetry.Models;

 namespace QuixStreams.Telemetry.Samples
{
    class Program
    {
        static void Main()
        {
            CodecRegistry.Register(CodecType.Json);
            PackageSerializationSettings.LegacyValueCodecType = TransportPackageValueCodecType.Json;
            PackageSerializationSettings.Mode = PackageSerializationMode.LegacyValue;

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };

            //var testBroker = new TestBroker();

            // Reading from Kafka
            TelemetryKafkaConsumer telemetryKafkaConsumer = new TelemetryKafkaConsumer(new TelemetryKafkaConsumerConfiguration(Configuration.Config.BrokerList, Configuration.Config.ConsumerId, Configuration.Config.Properties), Configuration.Config.Topic);
            //KafkaReader kafkaReader = new TestKafkaReader(testBroker);
            telemetryKafkaConsumer.ForEach(streamId =>
            {
                var s = new StreamPipeline(streamId)
                    .AddComponent(new SimpleModifier(69))
                    .AddComponent(new ConsoleStreamWriter());

                return s;
            });

            telemetryKafkaConsumer.Start();

            // Writing to Kafka (Random data)
            var randomDataReader = new RandomDataProducer();
            var stream = new StreamPipeline()
                .AddComponent(randomDataReader)
                //.AddComponent(new SimplyModifier(69))
                .AddComponent(new TelemetryKafkaProducer(KafkaHelper.OpenKafkaInput(new KafkaProducerConfiguration(Configuration.Config.BrokerList, Configuration.Config.Properties), Configuration.Config.Topic), null));
            //.AddComponent(new ConsoleStreamWriter()); // This is here to show if msg got correctly sent
                //.AddComponent(new TestKafkaProducer(testBroker));

            randomDataReader.Start(cts.Token);

        }

    }
}
