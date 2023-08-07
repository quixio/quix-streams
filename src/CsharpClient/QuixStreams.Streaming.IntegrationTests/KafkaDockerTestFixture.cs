using System;
using System.Diagnostics;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Extensions;
using Ductus.FluentDocker.Services;
using QuixStreams.IntegrationTestBase;
using QuixStreams.Streaming.Configuration;
using Xunit;

namespace QuixStreams.Streaming.IntegrationTests
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