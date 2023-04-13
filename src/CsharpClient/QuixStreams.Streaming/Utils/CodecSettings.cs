using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using QuixStreams.Telemetry.Models;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.Fw.Models;

namespace QuixStreams.Streaming.Utils
{
    /// <summary>
    /// Global Codec settings for streams.
    /// </summary>
    public static class CodecSettings
    {
        /// <summary>
        /// The currently configured codec
        /// </summary>
        public static CodecType CurrentCodec;

        /// <summary>
        /// The logger for the class
        /// </summary>
        private static Lazy<ILogger> logger = new Lazy<ILogger>(() => Logging.CreateLogger(typeof(CodecSettings)));

        static CodecSettings()
        {
            // Set the Json codec type as the default
            CodecSettings.SetGlobalCodecType(CodecType.Json);
        }
        
        /// <summary>
        /// Sets the codec type to be used by producers and transfer package value serialization 
        /// </summary>
        /// <param name="codecType"></param>
        public static void SetGlobalCodecType(CodecType codecType)
        {
            CodecRegistry.Register(producerCodec: codecType);
            
            if (codecType == CodecType.Protobuf)
            {
                SerializingModifier.PackageCodecType = TransportPackageValueCodecType.Binary;
            }
            else
            {
                SerializingModifier.PackageCodecType = TransportPackageValueCodecType.Json;
            }
            CurrentCodec = codecType;
            logger.Value.LogDebug("Codecs are configured to publish using {0} with {1} package codec.", codecType, SerializingModifier.PackageCodecType);
        }
    }
}