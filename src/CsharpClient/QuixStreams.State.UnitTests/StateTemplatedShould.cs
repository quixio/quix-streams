using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using FluentAssertions.Common;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.State.UnitTests
{
    public class StateTemplatedShould
    {
        [Fact]
        public void Constructor_ShouldCreateState()
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<int>(storage);

            state.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithNullStateStorage_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new DictionaryState<int>((IStateStorage)null);

            nullTopicName.Should().Throw<ArgumentNullException>().WithMessage("*storage*");
        }


        [Theory]
        [InlineData("TestKey", "TestValue")]
        public void StringConversion_ShouldStoreAndRetrieveCorrectly(string key, string value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<string>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<string>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 3.14)]
        public void DoubleConversion_ShouldStoreAndRetrieveCorrectly(string key, double value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<double>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<double>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 3.146546597897)]
        public void DecimalConversion_ShouldStoreAndRetrieveCorrectly(string key, decimal value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<decimal>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<decimal>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 42.0f)]
        public void FloatConversion_ShouldStoreAndRetrieveCorrectly(string key, float value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<float>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<float>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", true)]
        [InlineData("TestKey", false)]
        public void BoolConversion_ShouldStoreAndRetrieveCorrectly(string key, bool value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<bool>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<bool>(storage);

            state2[key].Should().Be(value);
        }


        [Theory]
        [InlineData("TestKey", 'A')]
        public void CharConversion_ShouldStoreAndRetrieveCorrectly(string key, char value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<char>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<char>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 1234567890123456789L)]
        public void LongConversion_ShouldStoreAndRetrieveCorrectly(string key, long value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<long>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<long>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 1234567890123456789UL)]
        public void UlongConversion_ShouldStoreAndRetrieveCorrectly(string key, ulong value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<ulong>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<ulong>(storage);

            state2[key].Should().Be(value);
        }


        [Theory]
        [InlineData("TestKey", 42)]
        public void IntConversion_ShouldStoreAndRetrieveCorrectly(string key, int value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<int>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<int>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", 42u)]
        public void UintConversion_ShouldStoreAndRetrieveCorrectly(string key, uint value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<uint>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<uint>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", (short)42)]
        public void ShortConversion_ShouldStoreAndRetrieveCorrectly(string key, short value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<short>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<short>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", (ushort)42u)]
        public void UshortConversion_ShouldStoreAndRetrieveCorrectly(string key, ushort value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<ushort>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<ushort>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", (byte)42)]
        public void ByteConversion_ShouldStoreAndRetrieveCorrectly(string key, byte value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<byte>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<byte>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestKey", (sbyte)42u)]
        public void SbyteConversion_ShouldStoreAndRetrieveCorrectly(string key, sbyte value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<sbyte>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<sbyte>(storage);

            state2[key].Should().Be(value);
        }

        [Theory]
        [MemberData(nameof(GetDateTimeValues))]
        public void DatetimeConversion_ShouldStoreAndRetrieveCorrectly(string key, DateTime value)
        {
            var storage = new InMemoryStorage();
            var state = new DictionaryState<DateTime>(storage);

            state[key] = value;

            state[key].Should().Be(value);

            state.Flush();

            var state2 = new DictionaryState<DateTime>(storage);

            state2[key].Should().Be(value);
        }

        public static IEnumerable<object[]> GetDateTimeValues()
        {
            yield return new object[] { "someKey", DateTime.UtcNow };
        }

        [Fact]
        public void CustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStorage();
            var key = "TestKey";
            var state = new DictionaryState<CustomClass>(storage);

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            state[key] = customObject;

            state[key].Should().BeEquivalentTo(customObject);

            state.Flush();

            var state2 = new DictionaryState<CustomClass>(storage);

            state2[key].Should().BeEquivalentTo(customObject);
        }

        [Fact]
        public void ListCustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStorage();
            var key = "TestKey";
            var state = new DictionaryState<List<CustomClass>>(storage);
            state.Clear();

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            var list = new List<CustomClass>();
            state[key] = list;
            state[key].Add(customObject);
            list.Add(customObject);
            list.Add(customObject);

            // No change is expected!
            state[key].Should().BeEquivalentTo(new List<CustomClass>());

            state[key] = list;

            state[key].Count.Should().Be(2);
            state[key].All(y => y.Equals(customObject)).Should().BeTrue();

            state.Flush();

            var state2 = new DictionaryState<List<CustomClass>>(storage);

            state2[key].Count.Should().Be(2);
            state2[key].All(y => y.IsSameOrEqualTo(customObject)).Should().BeTrue();
        }

        [Fact]
        public void OperationWithElement_ShouldStoreAndRetrieveCorrectly()
        {
            var storage = new InMemoryStorage();
            var key = "TestKey";
            var state = new DictionaryState<int>(storage);

            state[key] += 2;
            state[key] -= 1;
            state[key] *= 8;

            state[key].Should().Be(8);
        }


        [Fact]
        public void Clear_ShouldRemoveAllValues()
        {
            var storage = new InMemoryStorage();
            // Arrange
            var state = new DictionaryState<string>(storage);
            state.Add("Key1", "Value1");
            state.Add("Key2", "Value2");

            // Act
            state.Clear();
            state.Count.Should().Be(0);
            state.Flush();
            var state2 = new DictionaryState<string>(storage);

            // Assert
            state2.Count.Should().Be(0);
        }

        [Fact]
        public void ComplexModifications_ShouldHaveExpectedValues()
        {
            var storage = new InMemoryStorage();
            // Arrange
            var state = new DictionaryState<string>(storage);

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
            var state2 = new DictionaryState<string>(storage);

            // Assert 3
            state2.Count.Should().Be(3);
            state2["Key6"].Should().Be("Value6b");
            state2["Key7"].Should().Be("Value7");
            state2["Key8"].Should().Be("Value8");
        }

        [Fact]
        public void Reset_Modified_ShouldResetToSaved()
        {
            // Arrange
            var storage = new InMemoryStorage();
            var state = new DictionaryState<string>(storage);
            state.Add("key", "value");
            state.Flush();

            // Act
            state["key"] = "updatedValue";
            state.Reset();

            // Assert
            var value = state["key"];
            value.Should().BeEquivalentTo("value");
        }

        [Fact]
        public void Add_WithNullValue_ShouldRemoveFromState()
        {
            // Arrange
            var storage = new InMemoryStorage();
            var state = new DictionaryState<object>(storage);
            state.Add("key", "value");
            state.Flush();
            storage.ContainsKey("key").Should().BeTrue();

            // Act
            state["key"] = null;
            state.Flush();

            // Assert
            var value = state["key"];
            value.Should().BeNull();
            storage.ContainsKey("key").Should().BeFalse();
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