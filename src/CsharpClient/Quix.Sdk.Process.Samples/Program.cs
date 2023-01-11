﻿using Quix.Sdk.Process.Common.Test;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Process.Models;
using System;
using System.Collections.Generic;
 using System.IO;
 using System.Threading;
 using Confluent.Kafka;
 using Quix.Sdk.Process.Configuration;

 namespace Quix.Sdk.Process.Samples
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
            KafkaReader kafkaReader = new KafkaReader(new KafkaReaderConfiguration(Configuration.Config.BrokerList, Configuration.Config.ConsumerId, Configuration.Config.Properties), Configuration.Config.Topic);
            //KafkaReader kafkaReader = new TestKafkaReader(testBroker);
            kafkaReader.ForEach(streamId =>
            {
                var s = new StreamProcess(streamId)
                    .AddComponent(new SimplyModifier(69))
                    .AddComponent(new ConsoleStreamWriter());

                return s;
            });

            kafkaReader.Start();

            // Writing to Kafka (Random data)
            var randomDataReader = new RandomDataReader();
            var stream = new StreamProcess()
                .AddComponent(randomDataReader)
                //.AddComponent(new SimplyModifier(69))
                .AddComponent(new KafkaWriter(KafkaHelper.OpenKafkaInput(new KafkaWriterConfiguration(Configuration.Config.BrokerList, Configuration.Config.Properties), Configuration.Config.Topic)));
            //.AddComponent(new ConsoleStreamWriter()); // This is here to show if msg got correctly sent
                //.AddComponent(new TestKafkaWriter(testBroker));

            randomDataReader.Start(cts.Token);

        }

    }
}
