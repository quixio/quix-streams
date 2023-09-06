using System;
using FluentAssertions;
using NSubstitute;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using Xunit;

namespace QuixStreams.Kafka.Transport.Tests.SerDes.Codecs
{
    public class CodecRegistryShould
    {
        [Fact]
        public void RegisterThenRetrieve_NoPreviousRegistration_ShouldReturnRegisteredCodec()
        {
            // Arrange
            var codec = Substitute.For<ICodec>();
            codec.Id.Returns(new CodecId("TestCodec"));
            codec.Type.Returns(typeof(CodecRegistryShould));
            var modelKey = new ModelKey("Test");
            CodecRegistry.RegisterCodec(modelKey, codec);

            // Act
            var retrievedCodec = CodecRegistry.RetrieveCodec(modelKey, codec.Id);

            // Assert
            retrievedCodec.Should().Be(codec);
        }

        [Fact]
        public void RegisterThenRetrieve_HasPreviousRegistration_ShouldReturnRegisteredCodec()
        {
            // Arrange
            var codec = Substitute.For<ICodec>();
            codec.Id.Returns(new CodecId("TestCodec"));
            codec.Type.Returns(typeof(CodecRegistryShould));
            var codec2 = Substitute.For<ICodec>();
            codec2.Type.Returns(typeof(CodecRegistryShould));
            codec2.Id.Returns(new CodecId("TestCodec2"));
            var modelKey = new ModelKey("Test");
            CodecRegistry.RegisterCodec(modelKey, codec);
            // then register again
            CodecRegistry.RegisterCodec(modelKey, codec2);


            // Act
            var retrievedCodec = CodecRegistry.RetrieveCodec(modelKey, codec.Id);
            var retrievedCodec2 = CodecRegistry.RetrieveCodec(modelKey, codec2.Id);
            var allCodecs = CodecRegistry.RetrieveCodecs(modelKey);

            // Assert
            retrievedCodec.Should().Be(codec);
            retrievedCodec2.Should().Be(codec2);
            allCodecs.Should().BeEquivalentTo(new[] { codec2, codec }, (o) => o.WithStrictOrdering()); // the 2nd registered should be first
        }

        [Fact]
        public void ClearThenRetrieve_HasPreviousRegistration_ShouldReturnNull()
        {
            // Arrange
            var codec = Substitute.For<ICodec>();
            codec.Id.Returns(new CodecId("TestCodec"));
            codec.Type.Returns(typeof(CodecRegistryShould));
            var modelKey = new ModelKey("Test");
            CodecRegistry.RegisterCodec(modelKey, codec);


            // Act
            CodecRegistry.ClearCodecs(modelKey);
            var retrievedCodec = CodecRegistry.RetrieveCodec(modelKey, codec.Id);
            var allCodecs = CodecRegistry.RetrieveCodecs(modelKey);

            // Assert
            retrievedCodec.Should().Be(null);
            allCodecs.Should().BeEmpty();
        }

        [Fact]
        public void RegisterCodec_ValidCodec_ShouldAlsoRegisterInModelKeyRegistry()
        {
            // Arrange
            var codec = Substitute.For<ICodec>();
            codec.Id.Returns(new CodecId("TestCodec"));
            codec.Type.Returns(typeof(CodecRegistryShould));
            var modelKey = new ModelKey("Test");


            // Act
            CodecRegistry.RegisterCodec(modelKey, codec);
            var modelType = ModelKeyRegistry.GetType(modelKey);

            // Assert
            modelType.Should().Be(typeof(CodecRegistryShould));
        }

        [Fact]
        public void RegisterCodec_WithNullModelKey_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.RegisterCodec((string)null, Substitute.For<ICodec>());
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("modelKey");
        }

        [Fact]
        public void RegisterCodec_WithNullCodec_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.RegisterCodec(new ModelKey("Test"), null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("codec");
        }

        [Fact]
        public void ClearCodec_WithNullModelKey_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.ClearCodecs((string)null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("modelKey");
        }

        [Fact]
        public void RetrieveCodecs_WithNullModelKey_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.RetrieveCodecs((string)null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("modelKey");
        }

        [Fact]
        public void RetrieveCodec_WithNullModelKey_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.RetrieveCodec((string)null, new CodecId("CodecId"));
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("modelKey");
        }

        [Fact]
        public void RetrieveCodec_WithNullCodecId_ShouldThrowArgumentNullException()
        {
            Action action = () =>
            {
                CodecRegistry.RetrieveCodec(new ModelKey("Test"), null);
            };

            action.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("codecId");
        }

    }
}
