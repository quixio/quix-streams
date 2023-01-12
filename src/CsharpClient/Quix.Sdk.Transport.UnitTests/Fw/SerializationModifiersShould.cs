using FluentAssertions;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.UnitTests.Helpers;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests.Fw
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
            var package = new Package<TestModel>(new Lazy<TestModel>(modelValue), metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Send(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Arrange
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<TestModel>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Value.Should().BeEquivalentTo(modelValue);
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
            var package = new Package<TestModel[]>(new Lazy<TestModel[]>(modelValues), metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Send(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Arrange
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<TestModel[]>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Value.Should().BeEquivalentTo(modelValues);
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
            var package = new Package<byte[]>(new Lazy<byte[]>(value), metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Send(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Arrange
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<byte[]>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Value.Should().BeEquivalentTo(value);
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
        
        [Fact]
        public void DeserializingModifier_WithString_ShouldCorrespondToSerializingModifier()
        {
            // This test checks that DeserializingModifier and SerializingModifier are reverse of each other

            // Arrange
            var metaData = new MetaData(new Dictionary<string, string>() { { "Key1", "Value" } });
            var package = new Package<string>(new Lazy<string>("test string value"), metaData);
            var deserializingModifier = new DeserializingModifier();
            var serializingModifier = new SerializingModifier();

            serializingModifier.OnNewPackage += (package) =>
            {
                return deserializingModifier.Send(package);
            };

            Package deserializedPackage = null;
            deserializingModifier.OnNewPackage += (package) =>
            {
                deserializedPackage = package;
                return Task.CompletedTask;
            };


            // Act
            serializingModifier.Send(package).Wait(2000); // timout just in case test is failing

            // Arrange
            deserializedPackage.Should().NotBeNull();
            deserializedPackage.TryConvertTo<string>(out var convertedPackage).Should().BeTrue();
            convertedPackage.Value.Value.Should().BeEquivalentTo("test string value");
            convertedPackage.MetaData.Should().BeEquivalentTo(metaData);
        }
    }
}
