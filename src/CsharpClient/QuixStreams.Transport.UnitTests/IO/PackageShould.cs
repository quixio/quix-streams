using System;
using System.Threading;
using FluentAssertions;
using QuixStreams.Transport.IO;
using Xunit;

namespace QuixStreams.Transport.UnitTests.IO
{
    public class PackageShould
    {

        [Fact]
        public void Constructor_WithNullMetaData_ShouldSetEmptyMetaData()
        {
            // Act
            var package = new Package(typeof(object), new object(), null);

            // Assert
            package.MetaData.Should().NotBeNull();
            package.MetaData.Should().BeEquivalentTo(MetaData.Empty);
        }

        [Fact]
        public void Constructor_WithNullTransportContext_ShouldSetEmptyTransportContext()
        {
            // Act
            var package = new Package(typeof(object), new object(), null, null);

            // Assert
            package.TransportContext.Should().NotBeNull();
            package.TransportContext.Should().BeEmpty();
        }

        [Fact]
        public void Constructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new Package(typeof(PackageShould), null);

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void GenericConstructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new Package<PackageShould>(null);

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void Constructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new Package(typeof(object), 6);

            // Assert
            ((int)package.Value).Should().Be(6);
        }

        [Fact]
        public void GenericConstructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new Package<int>(6);

            // Assert
            package.Value.Should().Be(6);
            ((int)((Package)package).Value).Should().Be(6);
        }


        [Fact]
        public void GenericConstructor_ShouldNotEvaluateValueTwice()
        {
            // Act

            var package = new Package<int>(6);

            // Assert
            package.Value.Should().Be(6);
            ((int)((Package)package).Value).Should().Be(6);

        }


        [Fact]
        public void TryConvert_ToActualType_ShouldReturnTrue()
        {
            // Arrange
            var package = new Package<int>(6);
            var wrappedPackage = (Package)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<int>(out var convertedPackage);

            // Assert
            success.Should().BeTrue();
            convertedPackage.Should().Be(package); // should be the exact same package, because it isn't of derived type
        }

        [Fact]
        public void TryConvert_ToDerivedType_ShouldReturnTrue()
        {
            // Arrange
            var package = new Package<int>(6);
            var wrappedPackage = (Package)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<object>(out var convertedPackage);

            // Assert
            success.Should().BeTrue();
            convertedPackage.Should().NotBe(package); // not of same type
            ((int)convertedPackage.Value).Should().Be(6);
        }

        [Fact]
        public void TryConvert_ToIncorrectType_ShouldReturnFalse()
        {
            // Arrange
            var package = new Package<int>(6);
            var wrappedPackage = (Package)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<string>(out var convertedPackage);

            // Assert
            success.Should().BeFalse();
            convertedPackage.Should().BeNull();
        }
    }
}
