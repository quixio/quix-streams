using System;
using System.IO;
using FluentAssertions;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{
    public class AppShould
    {
    
        [Fact]
        public void GetStateStorageRootDir_WithoutSetStateStorageRootDir_ShouldNotThrowException()
        {
            // Act
            var defaultStorageRootDir = App.GetStateStorageRootDir();
            defaultStorageRootDir.Should().Be(Path.Combine(".", "state"));
        }
        
        [Fact]
        public void GetStateStorageType_WithoutSetStateStorageType_ShouldNotThrowException()
        {
            // Act
            var defaultStorageType = App.GetStateStorageType();
            defaultStorageType.Should().Be(StateStorageTypes.RocksDb);
        }
    
        [Fact(Skip = "Until reworked to use non-singleton only one of these tests will pass")]
        public void SetStateStorageRootDir_ShouldNotThrowException()
        {
            // Act
            App.SetStateStorageRootDir(Path.Combine(".", "state"));
        }
    
        [Fact(Skip = "Until reworked to use non-singleton only one of these tests will pass")]
        public void SetStateStorageRootDir_CalledTwice_ShouldThrowException()
        {
            // Arrange
            App.SetStateStorageRootDir(Path.Combine(".", "state"));
        
            // Act
            Action action = () => App.SetStateStorageRootDir(Path.Combine(".", "otherLocation"));
        
            // Assert
            action.Should().Throw<InvalidOperationException>();
        }
    }
}