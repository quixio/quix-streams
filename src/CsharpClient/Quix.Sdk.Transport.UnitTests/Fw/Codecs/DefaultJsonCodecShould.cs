using FluentAssertions;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests.Fw.Codecs
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
