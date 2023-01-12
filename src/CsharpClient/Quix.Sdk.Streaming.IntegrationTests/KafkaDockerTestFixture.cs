using System;
using System.Diagnostics;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;
using Ductus.FluentDocker.Services.Extensions;
using Quix.Sdk.Streaming.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Quix.Sdk.Streaming.IntegrationTests
{
    
    public class KafkaDockerTestFixture : IDisposable
    {
        private readonly IContainerService kafkaContainer;
        public readonly int ZookeeperPort;
        public readonly int KafkaPort;
        public readonly string BrokerList;
        public SecurityOptions SecurityOptions;

        public KafkaDockerTestFixture()
        {
            var random = new Random();
            var host = "127.0.0.1";
            this.ZookeeperPort = random.Next(1324,23457);
            this.KafkaPort = random.Next(23458,58787);
            this.BrokerList = $"{host}:{KafkaPort}";
            Console.WriteLine("Creating Kafka container");
            var sw = Stopwatch.StartNew();
            try
            {
                this.kafkaContainer = new Builder()
                    .UseContainer()
                    .UseImage("spotify/kafka")
                    .ExposePort(ZookeeperPort, 2181)
                    .ExposePort(KafkaPort, 9092)
                    .WithEnvironment($"ADVERTISED_HOST={host}")
                    .WithEnvironment($"ADVERTISED_PORT={KafkaPort}")
                    .Build()
                    .Start();
                this.kafkaContainer.WaitForRunning();
            }
            catch
            {
                this.kafkaContainer?.Dispose();
                throw;
            }

            Console.WriteLine("Created Kafka container in {0:g}", sw.Elapsed);
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing Kafka container");
            kafkaContainer.Dispose();
            Console.WriteLine("Disposed Kafka container");
        }

    }

    [CollectionDefinition("Kafka Container Collection")]
    public class KafkaDockerCollection : ICollectionFixture<KafkaDockerTestFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}