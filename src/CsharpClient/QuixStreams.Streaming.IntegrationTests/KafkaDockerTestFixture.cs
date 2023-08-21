using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;
using QuixStreams.IntegrationTestBase;
using QuixStreams.Streaming.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.IntegrationTests
{
    
    public class KafkaDockerTestFixture : KafkaDockerTestFixtureBase
    {
        
        public async Task EnsureTopic(string topic, int partitionCount)
        {
            try
            {
                await this.AdminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification() { Name = topic, NumPartitions = partitionCount } });
                Console.WriteLine($"Created topic {topic} with desired {partitionCount} partitions");
            }
            catch (Exception ex)
            {
                // it exists
            }

            var metadata = this.AdminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));
            var existingTopic = metadata.Topics.First();

            if (existingTopic.Partitions.Count == partitionCount)
            {
                Console.WriteLine($"Found topic {topic} with desired {partitionCount} partitions");
                return;
            }
            
            if (existingTopic.Partitions.Count > partitionCount) throw new InvalidOperationException("The topic has more partitions than required, you need to manually delete it first");
            
            try
            {
                Console.WriteLine($"Found topic {topic} with less than desired {partitionCount} partitions, creating up to {partitionCount}");
                await this.AdminClient.CreatePartitionsAsync(new PartitionsSpecification[] { new PartitionsSpecification() { Topic = topic, IncreaseTo = partitionCount } });
                await Task.Delay(100); // apparently waiting for the task above is not enough, so introducing some artificial delay
            }
            catch (CreatePartitionsException ex)
            {
                if (!ex.Message.Contains($"Topic already has {partitionCount} partitions")) throw;
            }
            await EnsureTopic(topic, partitionCount);
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