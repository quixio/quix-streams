using FluentAssertions;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests.Fw.Codecs
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
