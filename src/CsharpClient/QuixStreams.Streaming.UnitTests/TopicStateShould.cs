using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class TopicStateShould
    {
        [Fact]
        public void Constructor_ShouldCreateTopicState()
        {
            var topicState = new TopicState<int>("TestTopic", "TestStorage", key => -1);

            topicState.Should().NotBeNull();
        }

        [Fact]
        public void Constructor_WithNullOrWhitespaceTopicName_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new TopicState<int>(null, "TestStorage", key => -1);
            Action emptyTopicName = () => new TopicState<int>("", "TestStorage", key => -1);
            Action whitespaceTopicName = () => new TopicState<int>("    ", "TestStorage", key => -1);

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
        [InlineData("TestTopic", "TestStorage", "TestKey", "TestValue")]
        public void StringConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, string value)
        {
            var topicState = new TopicState<string>(topicName, storageName, k => "");

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 3.14)]
        public void DoubleConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, double value)
        {
            var topicState = new TopicState<double>(topicName, storageName, k => 0.0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 3.146546597897)]
        public void DecimalConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, decimal value)
        {
            var topicState = new TopicState<decimal>(topicName, storageName, k => (decimal)0.0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 42.0f)]
        public void FloatConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, float value)
        {
            var topicState = new TopicState<float>(topicName, storageName, k => 0.0f);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", true)]
        [InlineData("TestTopic", "TestStorage", "TestKey", false)]
        public void BoolConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, bool value)
        {
            var topicState = new TopicState<bool>(topicName, storageName, k => false);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }


        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 'A')]
        public void CharConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, char value)
        {
            var topicState = new TopicState<char>(topicName, storageName, k => '\0');

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }

        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 1234567890123456789L)]
        public void LongConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, long value)
        {
            var topicState = new TopicState<long>(topicName, storageName, k => 0L);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 1234567890123456789UL)]
        public void UlongConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, ulong value)
        {
            var topicState = new TopicState<ulong>(topicName, storageName, k => 0L);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 42)]
        public void IntConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, int value)
        {
            var topicState = new TopicState<int>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", 42u)]
        public void UintConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, uint value)
        {
            var topicState = new TopicState<uint>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", (short)42)]
        public void ShortConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, short value)
        {
            var topicState = new TopicState<short>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", (ushort)42u)]
        public void UshortConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, ushort value)
        {
            var topicState = new TopicState<ushort>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", (byte)42)]
        public void ByteConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, byte value)
        {
            var topicState = new TopicState<byte>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [InlineData("TestTopic", "TestStorage", "TestKey", (sbyte)42u)]
        public void SbyteConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, sbyte value)
        {
            var topicState = new TopicState<sbyte>(topicName, storageName, k => 0);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        [Theory]
        [MemberData(nameof(GetDateTimeValues))]
        public void DatetimeConversion_ShouldStoreAndRetrieveCorrectly(string topicName, string storageName, string key, DateTime value)
        {
            var topicState = new TopicState<DateTime>(topicName, storageName,null);

            topicState[key] = value;

            topicState[key].Should().Be(value);
        }
        
        public static IEnumerable<object[]> GetDateTimeValues()
        {
            yield return new object[] { "TestTopic", "TestStorage", "TestKey", DateTime.UtcNow };
        }
        
        [Fact]
        public void CustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = "TestStorage";
            var key = "TestKey";
            var topicState = new TopicState<CustomClass>(topicName, storageName, k => new CustomClass());

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            topicState[key] = customObject;

            topicState[key].Should().BeEquivalentTo(customObject);
        }
        
        [Fact]
        public void ListCustomClassConversion_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = "TestStorage";
            var key = "TestKey";
            var topicState = new TopicState<List<CustomClass>>(topicName, storageName, k => new List<CustomClass>());

            var customObject = new CustomClass { Id = 1, Name = "TestObject" };
            topicState[key].Add(customObject);
            topicState[key].Add(customObject);
            topicState[key].Add(customObject);
            topicState[key].Add(customObject);
            topicState[key].Add(customObject);

            topicState[key].Should().BeEquivalentTo(customObject);
        }
        
        [Fact]
        public void OperationWithElement_ShouldStoreAndRetrieveCorrectly()
        {
            var topicName = "TestTopic";
            var storageName = "TestStorage";
            var key = "TestKey";
            var topicState = new TopicState<int>(topicName, storageName, k => 0);

            topicState[key] += 2;
            topicState[key] -= 1;
            topicState[key] *= 8;

            topicState[key].Should().Be(8);
        }

        public class CustomClass
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}