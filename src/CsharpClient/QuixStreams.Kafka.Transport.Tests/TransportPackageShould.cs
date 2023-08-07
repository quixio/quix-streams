using FluentAssertions;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests
{
    public class PackageShould
    {
        [Fact]
        public void Constructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new TransportPackage(typeof(PackageShould), "someKey", null);

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void GenericConstructor_ShouldSetTypeToProvided()
        {
            // Act
            var package = new TransportPackage<PackageShould>("someKey", null);

            // Assert
            package.Type.Should().Be(typeof(PackageShould));
        }

        [Fact]
        public void Constructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new TransportPackage(typeof(object), "someKey", 6);

            // Assert
            ((int)package.Value).Should().Be(6);
        }

        [Fact]
        public void GenericConstructor_ShouldSetValueToProvided()
        {
            // Act
            var package = new TransportPackage<int>("someKey", 6);

            // Assert
            package.Value.Should().Be(6);
            ((int)((TransportPackage)package).Value).Should().Be(6);
        }


        [Fact]
        public void GenericConstructor_ShouldNotEvaluateValueTwice()
        {
            // Act

            var package = new TransportPackage<int>("someKey", 6);

            // Assert
            package.Value.Should().Be(6);
            ((int)((TransportPackage)package).Value).Should().Be(6);

        }


        [Fact]
        public void TryConvert_ToActualType_ShouldReturnTrue()
        {
            // Arrange
            var package = new TransportPackage<int>("someKey", 6);
            var wrappedPackage = (TransportPackage)(package);

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
            var package = new TransportPackage<int>("someKey", 6);
            var wrappedPackage = (TransportPackage)(package);

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
            var package = new TransportPackage<int>("someKey", 6);
            var wrappedPackage = (TransportPackage)(package);

            // Act
            var success = wrappedPackage.TryConvertTo<string>(out var convertedPackage);

            // Assert
            success.Should().BeFalse();
            convertedPackage.Should().BeNull();
        }
    }
}
