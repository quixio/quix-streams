using FluentAssertions;
using Xunit;

namespace QuixStreams.State.UnitTests
{
    public class StateValueShould
    {
        [Fact]
        public void TestLong()
        {
            var value = new StateValue(1);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Long);
            value.LongValue.Should().Be(1);
        }

        [Fact]
        public void TestBool()
        {
            var value = new StateValue(true);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Bool);
            value.BoolValue.Should().Be(true);
        }
        
        [Fact]
        public void TestString()
        {
            var value = new StateValue("TestStr312");
            value.Type.Should().BeEquivalentTo(StateValue.StateType.String);
            value.StringValue.Should().Be("TestStr312");
        }
        
        [Fact]
        public void TestString_WithNull_ShouldReturnNull()
        {
            var value = new StateValue((string)null);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.String);
            value.StringValue.Should().BeNull();
            value.IsNull().Should().BeTrue();
        }

        
        [Fact]
        public void TestDouble()
        {
            var value = new StateValue(0.426);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Double);
            value.DoubleValue.Should().Be(0.426);
        }
        [Fact]
        public void TestBinary()
        {
            var bytes = new byte[] { 0, 4, 6, 8, 1, 0, 43, 255, 0, 32 };

            var value = new StateValue(bytes);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Binary);
            value.BinaryValue.Should().BeEquivalentTo(bytes, options => options.WithStrictOrdering());
        }
        
        [Fact]
        public void TestBinary_WithNull_ShouldReturnNull()
        {
            var bytes = (byte[])null;

            var value = new StateValue(bytes);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Binary);
            value.BinaryValue.Should().BeNull();
            value.IsNull().Should().BeTrue();
        }
        
        [Fact]
        public void TestObject_WithValue_ShouldReturnValue()
        {
            var bytes = (byte[])null;

            var value = new StateValue(bytes, StateValue.StateType.Object);
            value.Type.Should().BeEquivalentTo(StateValue.StateType.Object);
            value.BinaryValue.Should().BeNull();
            value.IsNull().Should().BeTrue();
        }


    }
}
