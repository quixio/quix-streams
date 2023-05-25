using System;
using FluentAssertions;
using QuixStreams.State.Serializers;
using Xunit;

namespace QuixStreams.State.UnitTests.Serializers
{
    public class ByteValueSerializerShould
    {

        [Fact]
        public void TestBoolean()
        {
            var data = new StateValue(true);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            
            data.BoolValue.Should().Be(deserialized.BoolValue);
        }

        [Fact]
        public void TestString()
        {
            var data = new StateValue("123");
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            data.StringValue.Should().BeEquivalentTo(deserialized.StringValue);
        }

        [Fact]
        public void TestLong()
        {
            var data = new StateValue(12L);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            data.LongValue.Should().Be(deserialized.LongValue);
        }

        [Fact]
        public void TestBinary()
        {
            var data = new StateValue(new byte[] { 1,5,78,21 });
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            data.BinaryValue.Should().BeEquivalentTo(deserialized.BinaryValue);
        }

        [Fact]
        public void TestDouble()
        {
            var data = new StateValue(1.57);
            var serialized = ByteValueSerializer.Serialize(data);
            var deserialized = ByteValueSerializer.Deserialize(serialized);
            data.DoubleValue.Should().Be(deserialized.DoubleValue);
        }

    }
}
