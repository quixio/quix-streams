using FluentAssertions;
using Quix.Streams.Transport.Fw.Codecs;
using Quix.Streams.Transport.UnitTests.Helpers;
using Xunit;

namespace Quix.Streams.Transport.UnitTests.Fw.Codecs
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
