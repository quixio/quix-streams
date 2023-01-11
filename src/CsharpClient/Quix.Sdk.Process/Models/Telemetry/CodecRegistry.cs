﻿using Quix.Sdk.Transport.Fw.Codecs;
using System;
using System.Collections.Generic;
using System.Linq;
using Quix.Sdk.Process.Models.Codecs;
using Quix.Sdk.Process.Models.Telemetry.Parameters.Codecs;

namespace Quix.Sdk.Process.Models
{
    /// <summary>
    /// Registry main class to register all the available codecs.
    /// </summary>
    public static class CodecRegistry
    {
        /// <summary>
        /// Register all the codecs of the library.
        /// For reading processes the incoming codec will be used automatically if it's available in the registered codecs.
        /// </summary>
        /// <param name="writingCodec">Codec for writing processes</param>
        public static void Register(CodecType writingCodec = CodecType.Json)
        {
            foreach(CodecType codec in Enum.GetValues(typeof(CodecType)))
            {
                if (codec == writingCodec) continue;
                RegisterCodec(codec);
            }

            // Writing Codec
            RegisterCodec(writingCodec);
        }

        private static void RegisterCodec(CodecType codec)
        {
            RegisterType<StreamProperties>(codec);
            RegisterType<StreamEnd>(codec);
            RegisterType<ParameterDataRaw>(codec);
            RegisterType<ParameterDefinitions>(codec);
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
                        Transport.Registry.CodecRegistry.RegisterCodec(modelKey, new DefaultJsonCodec<TType>());   
                    }
                    break;
                case CodecType.ImprovedJson:
                    foreach (var modelKey in modelKeys)
                    {
                        if (typeof(ParameterDataRaw) == modelType)
                        {
                            // Register the better performing specific codecs also for writing/reading
                            Transport.Registry.CodecRegistry.RegisterCodec(modelKey, new ParameterDataJsonCodec());
                        }
                    }

                    break;
                case CodecType.Protobuf:
                    foreach (var modelKey in modelKeys)
                    {
                        if (typeof(ParameterDataRaw) == modelType)
                        {
                            // Register the better performing specific codecs also for writing/reading
                            Transport.Registry.CodecRegistry.RegisterCodec(modelKey, new ParameterDataProtobufCodec());
                        }
                    }

                    break;
                default:
                    throw new NotImplementedException($"Codec '{codec:G}' not implemented in Process.Models.RegistryCodecs.");
            }
        }
    }
}