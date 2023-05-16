using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.UnitTests.Helpers;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw
{
    public class SerializationModifiersShould
    {
        [Fact]
        public void DeserializingModifier_WithSingleObject_ShouldCorrespondToSerializingModifier()
        {
            // This test checks that DeserializingModifier and SerializingModifier are reverse of each other

            // Arrange
            var modelValue = TestModel.Create();
            var metaData = new MetaData(new Dictionary<string, string>() { { "Key1", "Value" } });
            var package = new Package<TestModel>(modelValue, metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Publish(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<TestModel>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Should().BeEquivalentTo(modelValue);
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
        
        [Fact]
        public void DeserializingModifier_WithArray_ShouldCorrespondToSerializingModifier()
        {
            // This test checks that DeserializingModifier and SerializingModifier are reverse of each other

            // Arrange
            var modelValues = new []
            {
                TestModel.Create(),
                TestModel.Create(),
                TestModel.Create()
            };
            var metaData = new MetaData(new Dictionary<string, string>() { { "Key1", "Value" } });
            var package = new Package<TestModel[]>(modelValues, metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Publish(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<TestModel[]>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Should().BeEquivalentTo(modelValues);
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
        
        [Fact]
        public void DeserializingModifier_WithByteArray_ShouldCorrespondToSerializingModifier()
        {
            // This test checks that DeserializingModifier and SerializingModifier are reverse of each other

            // Arrange
            var rand = new Random();
            var value = new byte[100];
            rand.NextBytes(value);
            var metaData = new MetaData(new Dictionary<string, string>() { { "Key1", "Value" } });
            var package = new Package<byte[]>(value, metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Publish(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<byte[]>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Should().BeEquivalentTo(value);
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
        
        [Fact]
        public void DeserializingModifier_WithString_ShouldCorrespondToSerializingModifier()
        {
            // This test checks that DeserializingModifier and SerializingModifier are reverse of each other

            // Arrange
            var metaData = new MetaData(new Dictionary<string, string>() { { "Key1", "Value" } });
            var package = new Package<string>("test string value", metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Publish(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<string>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Should().BeEquivalentTo("test string value");
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
        
        [Fact]
        public void DeserializingModifier_WithRawData_ShouldWork()
        {
            // This test checks that DeserializingModifier can handle raw (non-quix) messages
            
            // Arrange
            string packageMessage = "This is a raw message";
            var package = new Package<byte[]>(Encoding.UTF8.GetBytes(packageMessage));
            var deserializingModifier = new DeserializingModifier();
            
            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (newPackage) =>
            {
                deserializedPackage = newPackage;
                return Task.CompletedTask;
            };
            
            // Act
            deserializingModifier.Publish(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.Value.Should().BeEquivalentTo(Encoding.UTF8.GetBytes(packageMessage));
        }
        
        [Fact]
        public void DeserializingModifier_WithRawDataStartingWith0x01_ShouldWork()
        {
            // Arrange
            byte[] packageMessage = { 0x01, 0x02, 0x03 };
            var package = new Package<byte[]>(packageMessage);
            var deserializingModifier = new DeserializingModifier();
            
            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (newPackage) =>
            {
                deserializedPackage = newPackage;
                return Task.CompletedTask;
            };
            
            // Act
            deserializingModifier.Publish(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.Value.Should().BeEquivalentTo(packageMessage);
        }
        
        [Fact]
        public void DeserializingModifier_WithRawDataAsJson_ShouldWork()
        {
            // Arrange
            string packageMessage = "{\"a\":\"\"}";
            var package = new Package<byte[]>(Encoding.UTF8.GetBytes(packageMessage));
            var deserializingModifier = new DeserializingModifier();
            
            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (newPackage) =>
            {
                deserializedPackage = newPackage;
                return Task.CompletedTask;
            };
            
            // Act
            deserializingModifier.Publish(package).Wait(2000); // timout just in case test is failing

            // Assert
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.Value.Should().BeEquivalentTo(Encoding.UTF8.GetBytes(packageMessage));
        }
    }
}
