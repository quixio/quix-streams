using System;
using System.Threading;
using FluentAssertions;
using Quix.Sdk.Transport.IO;
using Xunit;

namespace Quix.Sdk.Transport.UnitTests.IO
{
    public class PackageShould
    {

        [Fact]
        public void Constructor_WithNullMetaData_ShouldSetEmptyMetaData()
        {
            // Act
            var package = new Package(typeof(object), new Lazy<object>(new object()), null);

            // Assert
            package.MetaData.Should().NotBeNull();
            package.MetaData.Should().BeEquivalentTo(MetaData.Empty);
        }

        [Fact]
        public void Constructor_WithNullTransportContext_ShouldSetEmptyTransportContext()
        {
            // Act
            var package = new Package(typeof(object), new Lazy<object>(new object()), null, null);

            // Assert
            package.TransportContext.Should().NotBeNull();
            package.TransportContext.Should().BeEmpty();
        }

        [Fact]
        public void Constructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new Package(typeof(PackageShould), new Lazy<object>(() => null));

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void GenericConstructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new Package<PackageShould>(new Lazy<PackageShould>(() => null));

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void Constructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new Package(typeof(object), new Lazy<object>(() => (object)(1 + 2 + 3)));

            // Assert
            ((int)package.Value.Value).Should().Be(6);
        }

        [Fact]
        public void GenericConstructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new Package<int>(new Lazy<int>(() => 1 + 2 + 3));

            // Assert
            package.Value.Value.Should().Be(6);
            ((int)((Package)package).Value.Value).Should().Be(6);
        }


        [Fact]
        public void GenericConstructor_ShouldNotEvaluateValueTwice()
        {
            // Act
            var counter = 0;

            var package = new Package<int>(new Lazy<int>(() =>
            {
                Interlocked.Increment(ref counter);
                return 1 + 2 + 3;
            }));

            // Act
            package.Value.Value.Should().Be(6);
            ((int)((Package)package).Value.Value).Should().Be(6);

            // Assert
            counter.Should().Be(1);
        }


        [Fact]
        public void TryConvert_ToActualType_ShouldReturnTrue()
        {
            // Arrange
            var package = new Package<int>(new Lazy<int>(() => 1 + 2 + 3));
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
            var package = new Package<int>(new Lazy<int>(() => 1 + 2 + 3));
            var wrappedPackage = (Package)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<object>(out var convertedPackage);

            // Assert
            success.Should().BeTrue();
            convertedPackage.Should().NotBe(package); // not of same type
            ((int)convertedPackage.Value.Value).Should().Be(6);
        }

        [Fact]
        public void TryConvert_ToIncorrectType_ShouldReturnFalse()
        {
            // Arrange
            var package = new Package<int>(new Lazy<int>(() => 1 + 2 + 3));
            var wrappedPackage = (Package)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<string>(out var convertedPackage);

            // Assert
            success.Should().BeFalse();
            convertedPackage.Should().BeNull();
        }
    }
}
