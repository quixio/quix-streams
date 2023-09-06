using System;
using System.Diagnostics;
using Confluent.Kafka;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;

namespace QuixStreams.IntegrationTestBase
{
    public class KafkaDockerTestFixtureBase : IDisposable
    {
        private readonly IContainerService kafkaContainer;
        public readonly int ZookeeperPort;
        public readonly int KafkaPort;
        public readonly string BrokerList;
        public readonly IAdminClient AdminClient;

        public KafkaDockerTestFixtureBase()
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
                var builder = new Builder().UseContainer();
                
                if (System.Runtime.InteropServices.RuntimeInformation.OSArchitecture == System.Runtime.InteropServices.Architecture.Arm64)
                {
                    builder = builder.UseImage("dougdonohoe/fast-data-dev:latest");
                }
                else if (System.Runtime.InteropServices.RuntimeInformation.OSArchitecture == System.Runtime.InteropServices.Architecture.X64)
                {
                    builder = builder.UseImage("lensesio/fast-data-dev:3.3.1");
                }
                
                builder.ExposePort(ZookeeperPort, ZookeeperPort)
                .ExposePort(KafkaPort, KafkaPort)
                .WithEnvironment(
                    $"BROKER_PORT={KafkaPort}", 
                    $"ZK_PORT={ZookeeperPort}",
                    $"ADV_HOST={host}",
                    "REST_PORT=0",
                    "WEB_PORT=0",
                    "CONNECT_PORT=0",
                    "REGISTRY_PORT=0",
                    "RUNTESTS=0",
                    "SAMPLEDATA=0",
                    "FORWARDLOGS=0",
                    "SUPERVISORWEB=0");
                this.kafkaContainer = builder.Build().Start();
                this.kafkaContainer.WaitForRunning();
            }
            catch
            {
                this.kafkaContainer?.Dispose();
                throw;
            }

            Console.WriteLine("Created Kafka container in {0:g}", sw.Elapsed);
            this.AdminClient = new AdminClientBuilder(new ConsumerConfig { BootstrapServers = BrokerList }).Build();

            sw.Restart();
            EnsureThereIsBroker(DateTime.UtcNow.AddSeconds(30));
            Console.WriteLine("Kafka took {0:g} to be reachable", sw.Elapsed);

        }
        
        private void EnsureThereIsBroker(DateTime waitUntilMax)
        {
            if (waitUntilMax <= DateTime.UtcNow) throw new Exception("Failed to connect to the broker");
            try
            {
                var result = AdminClient.GetMetadata(TimeSpan.FromSeconds(5));
            }
            catch (KafkaException ex)
            {
                if (ex.Message.Contains("Broker transport failure"))
                {
                    EnsureThereIsBroker(waitUntilMax);
                    return;
                }  
            }
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing Kafka container");
            AdminClient.Dispose();
            kafkaContainer.Dispose();
            Console.WriteLine("Disposed Kafka container");
        }

    }
}