using System.Linq;
using FluentAssertions;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Process.Models.Codecs;
using Quix.Sdk.Process.Models.Telemetry.Parameters.Codecs;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Fw.Codecs;
using Xunit;

namespace Quix.Sdk.Process.UnitTests.Models.Telemetry
{
    public class CodecRegistryShould
    {
        private void ValidateForDefaultJsonCodec<T>()
        {
            var codecs = Transport.Registry.CodecRegistry.RetrieveCodecs(new ModelKey(typeof(T).Name));
            var writeCodec = codecs.FirstOrDefault();
            writeCodec.Should().NotBeNull();
            writeCodec.GetType().IsAssignableFrom(typeof(DefaultJsonCodec<T>)).Should().BeTrue($"expecting DefaultJsonCodec<{typeof(T).Name}>");
        }
        
        [Fact]
        public void Register_JsonEvents_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            var codecs = Transport.Registry.CodecRegistry.RetrieveCodecs("EventData[]");
            var writeCodec = codecs.FirstOrDefault();
            writeCodec.Should().NotBeNull();
            writeCodec.GetType().IsAssignableFrom(typeof(DefaultJsonCodec<EventDataRaw[]>)).Should().BeTrue($"expecting DefaultJsonCodec<EventData[]>");
        }
        
        [Fact]
        public void Register_ImprovedJsonParameterData_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.ImprovedJson);
            
            // Assert
            var codecs = Transport.Registry.CodecRegistry.RetrieveCodecs(new ModelKey("ParameterData"));
            codecs.Count().Should().Be(3);
            codecs.Should().Contain(x => x is DefaultJsonCodec<ParameterDataRaw>); // for reading
            codecs.Should().Contain(x => x is ParameterDataJsonCodec);
            codecs.Should().Contain(x => x is ParameterDataProtobufCodec); // for reading
            codecs.First().GetType().Should().Be(typeof(ParameterDataJsonCodec)); // for writing
        }
        
        [Fact]
        public void Register_JsonParameterData_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            var codecs = Transport.Registry.CodecRegistry.RetrieveCodecs(new ModelKey("ParameterData"));
            codecs.Count().Should().Be(3);
            codecs.Should().Contain(x => x is DefaultJsonCodec<ParameterDataRaw>); 
            codecs.Should().Contain(x => x is ParameterDataJsonCodec); // for reading
            codecs.Should().Contain(x => x is ParameterDataProtobufCodec); // for reading
            codecs.First().GetType().Should().Be(typeof(DefaultJsonCodec<ParameterDataRaw>)); // for writing
        }
        
        [Fact]
        public void Register_JsonStreamProperties_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            ValidateForDefaultJsonCodec<StreamProperties>();
        }
        
        [Fact]
        public void Register_JsonParameterDefinitions_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            ValidateForDefaultJsonCodec<Process.Models.ParameterDefinitions>();
        }

        [Fact]
        public void Register_JsonEventDefinitions_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);

            // Assert
            ValidateForDefaultJsonCodec<Process.Models.EventDefinitions>();
        }


        [Fact]
        public void Register_JsonStreamEnd_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            ValidateForDefaultJsonCodec<StreamEnd>();
        }
    }
}