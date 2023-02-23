﻿using System;
 using System.Threading;
 using Quix.Streams.Process.Kafka;
 using Quix.Streams.Process.Models;

 namespace Quix.Streams.Process.Samples
{
    class Program
    {
        static void Main()
        {
            CodecRegistry.Register(CodecType.Protobuf);
            
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
                var s = new StreamProcess(streamId)
                    .AddComponent(new SimplyModifier(69))
                    .AddComponent(new ConsoleStreamWriter());

                return s;
            });

            telemetryKafkaConsumer.Start();

            // Writing to Kafka (Random data)
            var randomDataReader = new RandomDataReader();
            var stream = new StreamProcess()
                .AddComponent(randomDataReader)
                //.AddComponent(new SimplyModifier(69))
                .AddComponent(new TelemetryKafkaProducer(KafkaHelper.OpenKafkaInput(new KafkaWriterConfiguration(Configuration.Config.BrokerList, Configuration.Config.Properties), Configuration.Config.Topic), null));
            //.AddComponent(new ConsoleStreamWriter()); // This is here to show if msg got correctly sent
                //.AddComponent(new TestKafkaWriter(testBroker));

            randomDataReader.Start(cts.Token);

        }

    }
}
