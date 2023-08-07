using System;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes.Codecs
{
    public class ModelKeyRegistryShould
    {
        [Fact]
        public void Retrieve_AfterSuccessfulRegister_ShouldReturnExpected()
        {
            // Arrange
            var type = typeof(ModelKeyRegistryShould);
            var modelKey = new ModelKey(type, 32);
            ModelKeyRegistry.RegisterModel(type, modelKey);

            // Act
            var retrievedType = ModelKeyRegistry.GetType(modelKey);
            var retrievedModelKey = ModelKeyRegistry.GetModelKey(type);

            // Assert
            retrievedType.Should().Be(type);
            retrievedModelKey.Should().Be(modelKey);
        }

        [Fact]
        public void Retrieve_RegisterMultiple_ShouldReturnExpected()
        {
            // Arrange
            var type = typeof(ModelKeyRegistryShould);
            var modelKey = new ModelKey(type, 32);
            var modelKey2 = new ModelKey(type);
            ModelKeyRegistry.RegisterModel(type, modelKey);
            ModelKeyRegistry.RegisterModel(type, modelKey2);

            // Act
            var retrievedType = ModelKeyRegistry.GetType(modelKey);
            var retrievedType2 = ModelKeyRegistry.GetType(modelKey);
            var retrievedModelKey = ModelKeyRegistry.GetModelKey(type);

            // Assert
            retrievedType.Should().Be(type); // both should
            retrievedType2.Should().Be(type); // work, but
            retrievedModelKey.Should().Be(modelKey2); // second should wins for type
        }

        [Fact]
        public void GetModelKey_WithNullType_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                ModelKeyRegistry.GetModelKey(null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("type");
        }

        [Fact]
        public void GetType_WithNullModelKey_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                ModelKeyRegistry.GetType((string)null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("modelKey");
        }
    }
}
