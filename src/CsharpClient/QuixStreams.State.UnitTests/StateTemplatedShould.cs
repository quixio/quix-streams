using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using FluentAssertions.Common;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.State.UnitTests;

public class StateTemplatedShould
{ 
    public class StateShould
    {
        [Fact]
        public void Constructor_ShouldCreateState()
        {
            var storage = new InMemoryStateStorage();
            var state = new State<int>(storage);

            state.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithNullStateStorage_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new State<int>((IStateStorage)null);

            nullTopicName.Should().Throw<ArgumentNullException>().WithMessage("*storage*");
        }
        
        
        [Theory]
        [InlineData("TestKey", "TestValue")]
        public void StringConversion_ShouldStoreAndRetrieveCorrectly(string key, string value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<string>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<string>(storage);
            
            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 3.14)]
        public void DoubleConversion_ShouldStoreAndRetrieveCorrectly(string key, double value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<double>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<double>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", 3.146546597897)]
        public void DecimalConversion_ShouldStoreAndRetrieveCorrectly(string key, decimal value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<decimal>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 =  new State<decimal>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", 42.0f)]
        public void FloatConversion_ShouldStoreAndRetrieveCorrectly(string key, float value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<float>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 =  new State<float>(storage);
            
            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", true)]
        [InlineData("TestKey", false)]
        public void BoolConversion_ShouldStoreAndRetrieveCorrectly(string key, bool value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<bool>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<bool>(storage);
            
            state2[key].Should().Be(value);
        }


        [Theory]
        [InlineData("TestKey", 'A')]
        public void CharConversion_ShouldStoreAndRetrieveCorrectly(string key, char value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<char>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<char>(storage);
            
            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 1234567890123456789L)]
        public void LongConversion_ShouldStoreAndRetrieveCorrectly(string key, long value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<long>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<long>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", 1234567890123456789UL)]
        public void UlongConversion_ShouldStoreAndRetrieveCorrectly(string key, ulong value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<ulong>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();
            
            var state2 = new State<ulong>(storage);
            
            state2[key].Should().Be(value);
        }
        
        
        [Theory]
        [InlineData("TestKey", 42)]
        public void IntConversion_ShouldStoreAndRetrieveCorrectly(string key, int value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<int>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<int>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", 42u)]
        public void UintConversion_ShouldStoreAndRetrieveCorrectly(string key, uint value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<uint>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<uint>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", (short)42)]
        public void ShortConversion_ShouldStoreAndRetrieveCorrectly(string key, short value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<short>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<short>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", (ushort)42u)]
        public void UshortConversion_ShouldStoreAndRetrieveCorrectly(string key, ushort value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<ushort>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<ushort>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", (byte)42)]
        public void ByteConversion_ShouldStoreAndRetrieveCorrectly(string key, byte value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<byte>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<byte>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestKey", (sbyte)42u)]
        public void SbyteConversion_ShouldStoreAndRetrieveCorrectly(string key, sbyte value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<sbyte>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<sbyte>(storage);
            
            state2[key].Should().Be(value);
        }
        
        [Theory]
        [MemberData(nameof(GetDateTimeValues))]
        public void DatetimeConversion_ShouldStoreAndRetrieveCorrectly(string key, DateTime value)
        {
            var storage = new InMemoryStateStorage();
            var state = new State<DateTime>(storage);

            state[key] = value;

            state[key].Should().Be(value);
            
            state.Flush();
            
            var state2 = new State<DateTime>(storage);
            
            state2[key].Should().Be(value);
        }
        
        public static IEnumerable<object[]> GetDateTimeValues()
        {
            yield return new object[] { "someKey", DateTime.UtcNow};
        }
        
        [Fact]
        public void CustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStateStorage();
            var key = "TestKey";
            var state = new State<CustomClass>(storage);

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            state[key] = customObject;

            state[key].Should().BeEquivalentTo(customObject);
            
            state.Flush();
            
            var state2 = new State<CustomClass>(storage);
            
            state2[key].Should().BeEquivalentTo(customObject);
        }
        
        [Fact]
        public void ListCustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStateStorage();
            var key = "TestKey";
            var state = new State<List<CustomClass>>(storage);
            state.Clear();

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            state[key] = new List<CustomClass>();
            state[key].Add(customObject);
            state[key].Add(customObject);

            state[key].Count.Should().Be(2);
            state[key].All(y=> y == customObject).Should().BeTrue();
            
            state.Flush();
            
            var state2 = new State<List<CustomClass>>(storage);
            
            state2[key].Count.Should().Be(2);
            state2[key].All(y=> y.IsSameOrEqualTo(customObject)).Should().BeTrue();
        }
        
        [Fact]
        public void OperationWithElement_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStateStorage();
            var key = "TestKey";
            var state = new State<int>(storage);

            state[key] += 2;
            state[key] -= 1;
            state[key] *= 8;

            state[key].Should().Be(8);
        }
        
        
        [Fact]
        public void Clear_ShouldRemoveAllValues()
        {
            var storage = new InMemoryStateStorage();
            // Arrange
            var state = new State<string>(storage);
            state.Add("Key1", "Value1");
            state.Add("Key2", "Value2");

            // Act
            state.Clear();
            state.Count.Should().Be(0);
            state.Flush();
            var state2 = new State<string>(storage);

            // Assert
            state2.Count.Should().Be(0);
        }
        
        [Fact]
        public void ComplexModifications_ShouldHaveExpectedValues()
        {
            var storage = new InMemoryStateStorage();
            // Arrange
            var state = new State<string>(storage);
            
            // Act 1
            state.Add("Key1", "Value1"); // will keep as is, before clear
            state.Add("Key2", "Value2"); // will remove, before clear
            state.Add("Key3", "Value3"); // will override, before clear
            state["Key4"] = "Value4"; // leave as is
            state.Remove("Key2");
            state["Key3"] = "Value3b";
            state.Clear();
            state.Add("Key5", "Value5"); // will keep as is
            state.Add("Key6", "Value6"); // will remove
            state.Add("Key7", "Value7"); // will override
            state["Key8"] = "Value8"; // leave as is
            state.Remove("Key5");
            state["Key6"] = "Value6b";
            
            // Assert 1
            state.Count.Should().Be(3);
            state["Key6"].Should().Be("Value6b");
            state["Key7"].Should().Be("Value7");
            state["Key8"].Should().Be("Value8");
            
            // Act 2
            state.Flush();
            state["Key8"] = "Value8";
            state.Flush();
            
            // Assert 2
            state.Count.Should().Be(3);
            state["Key6"].Should().Be("Value6b");
            state["Key7"].Should().Be("Value7");
            state["Key8"].Should().Be("Value8");

            // Act 3
            var state2 = new State<string>(storage);

            // Assert 3
            state2.Count.Should().Be(3);
            state2["Key6"].Should().Be("Value6b");
            state2["Key7"].Should().Be("Value7");
            state2["Key8"].Should().Be("Value8");
        }

        public class CustomClass : IEquatable<CustomClass>
        {
            public int Id { get; set; }
            public string Name { get; set; }

            public bool Equals(CustomClass other)
            {
                if (other == null)
                {
                    return false;
                }

                return this.Id == other.Id && this.Name == other.Name;
            }

            public override bool Equals(object obj)
            {
                return this.Equals(obj as CustomClass);
            }

            public override int GetHashCode()
            {
                int hash = 17;
                hash = hash * 31 + this.Id.GetHashCode();
                hash = hash * 31 + (this.Name == null ? 0 : this.Name.GetHashCode());
                return hash;
            }
        }
    }
}