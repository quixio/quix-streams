using FluentAssertions;
using QuixStreams.Transport.Fw.Codecs;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw.Codecs
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
