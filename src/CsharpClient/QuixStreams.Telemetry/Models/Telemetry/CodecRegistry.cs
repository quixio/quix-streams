using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Telemetry.Models.Codecs;
using QuixStreams.Telemetry.Models.Telemetry.Parameters.Codecs;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Registry main class to register all the available codecs.
    /// </summary>
    public static class CodecRegistry
    {
        /// <summary>
        /// Register all the codecs for serialization and deserialization.
        /// For reading processes the incoming codec will be used automatically if it's available in the registered codecs.
        /// Note: The order of codec registry is important. The last codec registered will be used for writing.
        /// </summary>
        /// <param name="producerCodec">Codec to use by producers</param>
        public static void Register(CodecType producerCodec = CodecType.Json)
        {
            // Consumer codecs
            foreach(CodecType codec in Enum.GetValues(typeof(CodecType)))
            {
                if (codec == producerCodec) continue;
                RegisterCodec(codec);
            }

            // Producer codec
            RegisterCodec(producerCodec);
        }

        private static void RegisterCodec(CodecType codec)
        {
            RegisterType<StreamProperties>(codec);
            RegisterType<StreamEnd>(codec);
            RegisterType<TimeseriesDataRaw>(codec);
            RegisterType<ParameterDefinitions>(codec);
            RegisterType<EventDataRaw>(codec);
            RegisterType<EventDataRaw[]>(codec);
            RegisterType<List<EventDataRaw>>(codec);
            RegisterType<EventDefinitions>(codec);
        }

        private static void RegisterType<TType>(CodecType codec)
        {
            var modelType = typeof(TType);
            ICollection<string> modelKeys = new [] {typeof(TType).Name};
            var isArray = modelType.IsArray;
            if (isArray)
            {
                var underlyingType = modelType.GetElementType();
                if (underlyingType != null) modelType = underlyingType;
            }

            var mkAttribute = modelType.GetCustomAttributes(false).FirstOrDefault(x => x is ModelKeyAttribute) as ModelKeyAttribute;
            if (mkAttribute != null)
            {
                modelKeys = mkAttribute.ModelKeysForRead?.ToList() ?? new List<string>();
                modelKeys.Add(mkAttribute.ModelKeyForWrite);
                if (isArray)
                {
                    modelKeys = modelKeys.Select(y => y + "[]").ToList();
                }
            }
            
            switch (codec)
            {
                case CodecType.Json:
                    foreach (var modelKey in modelKeys)
                    {
                        QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(modelKey, new DefaultJsonCodec<TType>());   
                    }
                    break;
                case CodecType.CompactJsonForBetterPerformance:
                    foreach (var modelKey in modelKeys)
                    {
                        if (typeof(TimeseriesDataRaw) == modelType)
                        {
                            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(modelKey, new TimeseriesDataReadableCodec());
                            continue;
                        }
                        
                        QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(modelKey, new DefaultJsonCodec<TType>());
                    }

                    break;
                case CodecType.Protobuf:
                    foreach (var modelKey in modelKeys)
                    {
                        if (typeof(TimeseriesDataRaw) == modelType)
                        {
                            // Register the better performing specific codecs also for writing/reading
                            QuixStreams.Kafka.Transport.SerDes.Codecs.CodecRegistry.RegisterCodec(modelKey, new TimeseriesDataProtobufCodec());
                        }
                    }

                    break;
                default:
                    throw new NotImplementedException($"Codec '{codec:G}' not implemented in Telemetry.Models.RegistryCodecs.");
            }
        }
    }
}