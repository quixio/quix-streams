using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes.Codecs.DefaultCodecs
{
    public class StringCodecShould
    {
        [Fact]
        public void Serialize_Deserialize_ShouldReturnInputString()
        {
            // Arrange
            var str = "How much wood would a wood chuck chuck if a wood chuck could chuck wood?";
            // Act
            var serialized = StringCodec.Instance.Serialize(str);

            var deserialized = StringCodec.Instance.Deserialize(serialized);

            // Asssert

            deserialized.Should().BeEquivalentTo(str);
            
        }
    }
}
