using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using FluentAssertions;
using FluentAssertions.Common;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class TopicStateShould
    {
        private string GetConsistentStorageName([CallerMemberName] string methodName = "")
        {
            return methodName;
        }

        [Fact]
        public void Constructor_ShouldCreateTopicState()
        {
            var topicState = new TopicState<int>("TestTopic", GetConsistentStorageName(), key => -1);

            topicState.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithNullOrWhitespaceTopicName_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new TopicState<int>(null, GetConsistentStorageName(), key => -1);
            Action emptyTopicName = () => new TopicState<int>("", GetConsistentStorageName(), key => -1);
            Action whitespaceTopicName = () => new TopicState<int>("    ", GetConsistentStorageName(), key => -1);

            nullTopicName.Should().Throw<ArgumentNullException>().WithMessage("*topicName*");
            emptyTopicName.Should().Throw<ArgumentNullException>().WithMessage("*topicName*");
            whitespaceTopicName.Should().Throw<ArgumentNullException>().WithMessage("*topicName*");
        }
        
        [Fact]
        public void Constructor_WithNullOrWhitespaceStorageName_ShouldThrowArgumentNullException()
        {
            Action nullStorageName = () => new TopicState<int>("TestTopic", null, key => -1);
            Action emptyStorageName = () => new TopicState<int>("TestTopic", "", key => -1);
            Action whitespaceStorageName = () => new TopicState<int>("TestTopic", "    ", key => -1);

            nullStorageName.Should().Throw<ArgumentNullException>().WithMessage("*storageName*");
            emptyStorageName.Should().Throw<ArgumentNullException>().WithMessage("*storageName*");
            whitespaceStorageName.Should().Throw<ArgumentNullException>().WithMessage("*storageName*");
        }
        
        
        [Theory]
        [InlineData("TestTopic", "TestKey", "TestValue")]
        public void StringConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, string value)
        {
            var topicState = new TopicState<string>(topicName, GetConsistentStorageName(), k => "");

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<string>(topicName, GetConsistentStorageName(), k => "");
            
            topicState2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestKey", 3.14)]
        public void DoubleConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, double value)
        {
            var topicState = new TopicState<double>(topicName, GetConsistentStorageName(), k => 0.0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<double>(topicName, GetConsistentStorageName(), k => 0.0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", 3.146546597897)]
        public void DecimalConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, decimal value)
        {
            var topicState = new TopicState<decimal>(topicName, GetConsistentStorageName(), k => (decimal)0.0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 =  new TopicState<decimal>(topicName, GetConsistentStorageName(), k => (decimal)0.0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", 42.0f)]
        public void FloatConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, float value)
        {
            var topicState = new TopicState<float>(topicName, GetConsistentStorageName(), k => 0.0f);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 =  new TopicState<float>(topicName, GetConsistentStorageName(), k => 0.0f);
            
            topicState2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestKey", true)]
        [InlineData("TestTopic", "TestKey", false)]
        public void BoolConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, bool value)
        {
            var topicState = new TopicState<bool>(topicName, GetConsistentStorageName(), k => false);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<bool>(topicName, GetConsistentStorageName(), k => false);
            
            topicState2[key].Should().Be(value);
        }


        [Theory]
        [InlineData("TestTopic", "TestKey", 'A')]
        public void CharConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, char value)
        {
            var topicState = new TopicState<char>(topicName, GetConsistentStorageName(), k => '\0');

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<char>(topicName, GetConsistentStorageName(), k => '\0');
            
            topicState2[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestKey", 1234567890123456789L)]
        public void LongConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, long value)
        {
            var topicState = new TopicState<long>(topicName, GetConsistentStorageName(), k => 0L);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<long>(topicName, GetConsistentStorageName(), k => 0L);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", 1234567890123456789UL)]
        public void UlongConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, ulong value)
        {
            var topicState = new TopicState<ulong>(topicName, GetConsistentStorageName(), k => 0L);

            topicState[key] = value;

            topicState[key].Should().Be(value);

            topicState.Flush();
            
            var topicState2 = new TopicState<ulong>(topicName, GetConsistentStorageName(), k => 0L);
            
            topicState2[key].Should().Be(value);
        }
        
        
        [Theory]
        [InlineData("TestTopic", "TestKey", 42)]
        public void IntConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, int value)
        {
            var topicState = new TopicState<int>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<int>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", 42u)]
        public void UintConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, uint value)
        {
            var topicState = new TopicState<uint>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<uint>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", (short)42)]
        public void ShortConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, short value)
        {
            var topicState = new TopicState<short>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<short>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", (ushort)42u)]
        public void UshortConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, ushort value)
        {
            var topicState = new TopicState<ushort>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<ushort>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", (byte)42)]
        public void ByteConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, byte value)
        {
            var topicState = new TopicState<byte>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<byte>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestKey", (sbyte)42u)]
        public void SbyteConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, sbyte value)
        {
            var topicState = new TopicState<sbyte>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<sbyte>(topicName, GetConsistentStorageName(), k => 0);
            
            topicState2[key].Should().Be(value);
        }
        
        [Theory]
        [MemberData(nameof(GetDateTimeValues))]
        public void DatetimeConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string key, DateTime value)
        {
            var topicState = new TopicState<DateTime>(topicName, GetConsistentStorageName(),null);

            topicState[key] = value;

            topicState[key].Should().Be(value);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<DateTime>(topicName, GetConsistentStorageName(),null);
            
            topicState2[key].Should().Be(value);
        }
        
        public static IEnumerable<object[]> GetDateTimeValues()
        {
            yield return new object[] { "TestTopic", "TestKey", DateTime.UtcNow };
        }
        
        [Fact]
        public void CustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = GetConsistentStorageName();
            var key = "TestKey";
            var topicState = new TopicState<CustomClass>(topicName, GetConsistentStorageName(), null);

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            topicState[key] = customObject;

            topicState[key].Should().BeEquivalentTo(customObject);
            
            topicState.Flush();
            
            var topicState2 = new TopicState<CustomClass>(topicName, GetConsistentStorageName(), null);
            
            topicState2[key].Should().BeEquivalentTo(customObject);
        }
        
        [Fact]
        public void ListCustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = GetConsistentStorageName();
            var key = "TestKey";
            var topicState = new TopicState<List<CustomClass>>(topicName, GetConsistentStorageName(), k => new List<CustomClass>());
            topicState.Clear();

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            topicState[key].Add(customObject);
            topicState[key].Add(customObject);

            topicState[key].Count.Should().Be(2);
            topicState[key].All(y=> y == customObject).Should().BeTrue();
            
            topicState.Flush();
            
            var topicState2 = new TopicState<List<CustomClass>>(topicName, GetConsistentStorageName(), k => new List<CustomClass>());
            
            topicState2[key].Count.Should().Be(2);
            topicState2[key].All(y=> y.IsSameOrEqualTo(customObject)).Should().BeTrue();
        }
        
        [Fact]
        public void OperationWithElement_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = GetConsistentStorageName();
            var key = "TestKey";
            var topicState = new TopicState<int>(topicName, GetConsistentStorageName(), k => 0);

            topicState[key] += 2;
            topicState[key] -= 1;
            topicState[key] *= 8;

            topicState[key].Should().Be(8);
        }
        
        
        [Fact]
        public void Clear_ShouldRemoveAllValues()
        {
            // Arrange
            var topicState = new TopicState<string>("TestTopic", GetConsistentStorageName(), null);
            topicState.Add("Key1", "Value1");
            topicState.Add("Key2", "Value2");

            // Act
            topicState.Clear();
            topicState.Count.Should().Be(0);
            topicState.Flush();
            var topicState2 = new TopicState<string>("TestTopic", GetConsistentStorageName(), null);

            // Assert
            topicState2.Count.Should().Be(0);
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