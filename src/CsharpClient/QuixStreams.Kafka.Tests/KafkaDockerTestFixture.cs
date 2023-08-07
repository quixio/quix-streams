using QuixStreams.IntegrationTestBase;
using Xunit;

namespace QuixStreams.Transport.Kafka.Tests
{
    
    public class KafkaDockerTestFixture : KafkaDockerTestFixtureBase
    {
    }

    [CollectionDefinition("Kafka Container Collection")]
    public class KafkaDockerCollection : ICollectionFixture<KafkaDockerTestFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}