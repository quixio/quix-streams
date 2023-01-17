﻿using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;
using Quix.Sdk.Transport.Registry;
using Timer = System.Timers.Timer;

namespace Quix.Sdk.Transport.Samples.Samples
{
    /// <summary>
    ///     Example for simple package (Typed messages) reading
    ///     Note: Works well with WritePackages example
    /// </summary>
    public class ReadPackages
    {
        private const string TopicName = Const.PackageTopic;
        private const string InputGroup = "Test-Subscriber#2";
        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        ///     Start the reading which is an asynchronous process. See <see cref="NewPackageHandler" />
        /// </summary>
        /// <returns>Disposable output</returns>
        public IOutput Start()
        {
            this.RegisterCodecs();
            var output = this.CreateOutput();
            this.HookUpStatistics();
            output.OnNewPackage = this.NewPackageHandler;
            return output;
        }

        private Task NewPackageHandler(Package package)
        {
            if (!package.TryConvertTo<ExampleModel>(out var mPackage)) return Task.CompletedTask;
            Interlocked.Increment(ref this.consumedCounter);
            var key = mPackage.GetKey();
            // keep in mind value is lazily evaluated, so this is a position where one can decide whether to use it
            var value = mPackage.Value.Value;
            var packageMetaData = mPackage.MetaData;
            var timestamp = mPackage.MetaData.TryGetValue("DateTime", out var dts) ? (DateTime?) DateTime.Parse(dts) : null;
            return Task.CompletedTask;
        }

        private void RegisterCodecs()
        {
            // Regardless of how the example model is sent, this will let us read them
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            CodecRegistry.RegisterCodec("EM", new DefaultJsonCodec<ExampleModel>());
        }

        private IOutput CreateOutput()
        {
            var subConfig = new SubscriberConfiguration(Const.BrokerList, InputGroup);
            var topicConfig = new OutputTopicConfiguration(TopicName);
            var kafkaOutput = new KafkaOutput(subConfig, topicConfig);
            kafkaOutput.ErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            kafkaOutput.Open();
            var output = new TransportOutput(kafkaOutput);
            return output;
        }

        private void HookUpStatistics()
        {
            var sw = Stopwatch.StartNew();

            var timer = new Timer
            {
                AutoReset = false,
                Interval = 1000
            };

            timer.Elapsed += (s, e) =>
            {
                var elapsed = sw.Elapsed;
                var consumed = Interlocked.Read(ref this.consumedCounter);


                var consumedPerMin = consumed / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Consumed Packages: {consumed:N0}, {consumedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}