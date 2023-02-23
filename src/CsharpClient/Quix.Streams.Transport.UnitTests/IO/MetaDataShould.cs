using System.Collections.Generic;
using FluentAssertions;
using Quix.Streams.Transport.IO;
using Xunit;

namespace Quix.Streams.Transport.UnitTests.IO
{
    public class MetaDataShould
    {
        [Fact]
        public void Constructor_WithMultipleDictionaries_ShouldSetToExpected()
        {
            // Arrange
            var dictionary1 = new Dictionary<string, string>()
            {
                { "Key1", "value1" }
            };

            var dictionary2 = new Dictionary<string, string>()
            {
                { "Key2", "value1" },
                { "Key3", "value1" }
            };

            var dictionary3 = new Dictionary<string, string>()
            {
                { "Key3", "value2" }
            };

            // Act
            var metadata = new MetaData(dictionary1, dictionary2, dictionary3);

            // Assert
            metadata.Should().BeEquivalentTo(new Dictionary<string, string>()
            {
                { "Key1", "value1" },
                { "Key2", "value1" },
                { "Key3", "value2" }
            });
        }

        [Fact]
        public void Constructor_WithDictionaryThenModifyDictionary_ShouldNotUpdateMetaData()
        {
            // Arrange
            var dictionary = new Dictionary<string, string>()
            {
                { "Key1", "value1" }
            };
            var metadata = new MetaData(dictionary);


            // Act
            dictionary.Add("key2", "value1");
            dictionary["Key1"] = "value2";

            // Assert
            metadata.Should().BeEquivalentTo(new Dictionary<string, string>()
            {
                { "Key1", "value1" }
            });
        }

        [Fact]
        public void Constructor_WithNullDictionary_ShouldNotThrowException()
        {
            // Arrange
            Dictionary<string, string> dictionary = null;

            // Act
            new MetaData(dictionary);

            // Assert by not getting an exception
        }
    }
}
