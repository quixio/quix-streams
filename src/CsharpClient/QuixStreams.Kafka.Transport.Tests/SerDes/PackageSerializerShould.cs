using System;
using System.Text;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.Tests.Helpers;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes
{
    public class PackageSerializerShould
    {
        [Fact]
        public void deserializer_WithSingleObject_ShouldCorrespondToserializer()
        {
            // This test checks that deserializer and serializer are reverse of each other

            // Arrange
            var modelValue = TestModel.Create();
            var transportPackage = new TransportPackage<TestModel>("somekey", modelValue);
            var deserializer = new PackageDeserializer();
            var serializer = new PackageSerializer();

            // Act
            var serialized = serializer.Serialize(transportPackage);
            var deserialized = deserializer.Deserialize(serialized);

            // Assert
            deserialized.TryConvertTo<TestModel>(out var converted).Should().BeTrue();
            converted.Value.Should().BeEquivalentTo(modelValue);
        }
        
        [Fact]
        public void deserializer_WithArray_ShouldCorrespondToserializer()
        {
            // This test checks that deserializer and serializer are reverse of each other

            // Arrange
            var modelValues = new []
            {
                TestModel.Create(),
                TestModel.Create(),
                TestModel.Create()
            };
            var transportPackage = new TransportPackage<TestModel[]>("somekey", modelValues);
            var deserializer = new PackageDeserializer();
            var serializer = new PackageSerializer();

            // Act
            var serialized = serializer.Serialize(transportPackage);
            var deserialized = deserializer.Deserialize(serialized);

            // Assert
            deserialized.TryConvertTo<TestModel[]>(out var converted).Should().BeTrue();
            converted.Value.Should().BeEquivalentTo(modelValues);
        }
        
        [Fact]
        public void deserializer_WithByteArray_ShouldCorrespondToserializer()
        {
            // This test checks that deserializer and serializer are reverse of each other

            // Arrange
            var rand = new Random();
            var value = new byte[100];
            rand.NextBytes(value);
            var transportPackage = new TransportPackage<byte[]>("somekey", value);
            var deserializer = new PackageDeserializer();
            var serializer = new PackageSerializer();

            // Act
            var serialized = serializer.Serialize(transportPackage);
            var deserialized = deserializer.Deserialize(serialized);

            // Assert
            deserialized.TryConvertTo<byte[]>(out var converted).Should().BeTrue();
            converted.Value.Should().BeEquivalentTo(value);
        }
        
        [Fact]
        public void deserializer_WithString_ShouldCorrespondToserializer()
        {
            // This test checks that deserializer and serializer are reverse of each other

            // Arrange
            var transportPackage = new TransportPackage<string>("somekey", "test string value");
            var deserializer = new PackageDeserializer();
            var serializer = new PackageSerializer();

            // Act
            var serialized = serializer.Serialize(transportPackage);
            var deserialized = deserializer.Deserialize(serialized);

            // Assert
            deserialized.TryConvertTo<string>(out var converted).Should().BeTrue();
            converted.Value.Should().BeEquivalentTo("test string value");
        }
        
        [Fact]
        public void deserializer_WithRawData_ShouldWork()
        {
            // This test checks that deserializer can handle raw (non-quix) messages
            
            // Arrange
            string TransportPackageMessage = "TransportPackage message";
            var deserializer = new PackageDeserializer();
            
            // Act
            var deserialized = deserializer.Deserialize(new KafkaMessage(null, Encoding.UTF8.GetBytes(TransportPackageMessage), null));

            // Assert
            deserialized.Should().NotBeNull();
            deserialized.Value.Should().BeEquivalentTo(Encoding.UTF8.GetBytes(TransportPackageMessage));
        }
        
        [Fact]
        public void deserializer_WithRawDataStartingWith0x01_ShouldWork()
        {
            // Arrange
            byte[] TransportPackageMessage = { 0x01, 0x02, 0x03 };
            var deserializer = new PackageDeserializer();
            
            // Act
            var deserialized = deserializer.Deserialize(new KafkaMessage(null, TransportPackageMessage, null));

            // Assert
            deserialized.Should().NotBeNull();
            deserialized.Value.Should().BeEquivalentTo(TransportPackageMessage);
        }
        
        [Fact]
        public void deserializer_WithRawDataAsJson_ShouldWork()
        {
            // Arrange
            string TransportPackageMessage = "{\"a\":\"\"}";
            var deserializer = new PackageDeserializer();

            // Act
            var deserialized = deserializer.Deserialize(new KafkaMessage(null, Encoding.UTF8.GetBytes(TransportPackageMessage), null));

            // Assert
            deserialized.Should().NotBeNull();
            deserialized.Value.Should().BeEquivalentTo(Encoding.UTF8.GetBytes(TransportPackageMessage));
        }
    }
}
