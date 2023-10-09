using System.Linq;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Codecs;
using QuixStreams.Telemetry.Models.Telemetry.Parameters.Codecs;
using Xunit;
using CodecRegistry = QuixStreams.Telemetry.Models.CodecRegistry;

namespace QuixStreams.Telemetry.UnitTests.Models.Telemetry
{
    public class CodecRegistryShould
    {
        private void ValidateForDefaultJsonCodec<T>()
        {
            var codecs = QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RetrieveCodecs(new ModelKey(typeof(T).Name));
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
            var codecs = QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RetrieveCodecs("EventData[]");
            var writeCodec = codecs.FirstOrDefault();
            writeCodec.Should().NotBeNull();
            writeCodec.GetType().IsAssignableFrom(typeof(DefaultJsonCodec<EventDataRaw[]>)).Should().BeTrue($"expecting DefaultJsonCodec<EventData[]>");
        }
        
        [Fact]
        public void Register_CompactJsonForBetterPerformance_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.CompactJsonForBetterPerformance);
            
            // Assert
            var codecs = QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RetrieveCodecs(new ModelKey("TimeseriesData"));
            codecs.Count().Should().Be(3, $"{string.Join(", ", codecs.Select(y=> y.Id))} should contain 3");
            codecs.Should().Contain(x => x is TimeseriesDataReadableCodec); // for reading
            codecs.Should().Contain(x => x is DefaultJsonCodec<TimeseriesDataRaw>); // for reading
            codecs.Should().Contain(x => x is TimeseriesDataProtobufCodec); // for reading
            codecs.First().GetType().Should().Be(typeof(TimeseriesDataReadableCodec)); // for writing
        }
        
        [Fact]
        public void Register_JsonTimeseriesData_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);
            
            // Assert
            var codecs = QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RetrieveCodecs(new ModelKey("TimeseriesData"));
            codecs.Count().Should().Be(3);
            codecs.Should().Contain(x => x is TimeseriesDataReadableCodec); // for reading
            codecs.Should().Contain(x => x is DefaultJsonCodec<TimeseriesDataRaw>); // for reading
            codecs.Should().Contain(x => x is TimeseriesDataProtobufCodec); // for reading
            codecs.First().GetType().Should().Be(typeof(DefaultJsonCodec<TimeseriesDataRaw>)); // for writing
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
            ValidateForDefaultJsonCodec<ParameterDefinitions>();
        }

        [Fact]
        public void Register_JsonEventDefinitions_ShouldRegisterAsExpected()
        {
            // Act
            CodecRegistry.Register(CodecType.Json);

            // Assert
            ValidateForDefaultJsonCodec<EventDefinitions>();
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