using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes.Codecs.DefaultCodecs
{
    public class DefaultJsonCodecShould
    {
        [Fact]
        public void Serialize_Deserialize_ShouldReturnInputModel()
        {
            // Arrange
            var codec = new DefaultJsonCodec<TestModel>();

            var model = TestModel.Create();

            // Act
            var serialized = codec.Serialize(model);

            var deserialized = codec.Deserialize(serialized);

            // Asssert

            deserialized.Should().BeEquivalentTo(model);
            
        }
    }
}
