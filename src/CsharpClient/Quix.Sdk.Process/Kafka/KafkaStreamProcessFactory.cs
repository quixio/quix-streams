using System;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.Process.Kafka
{
    /// <summary>
    /// <see cref="StreamProcessFactory"/> implemented for Kafka.
    /// It takes the StreamId from Message Key instead of a Metadata parameter from the default implementation
    /// </summary>
    internal class KafkaStreamProcessFactory : StreamProcessFactory
    {
        public KafkaStreamProcessFactory(IOutput transportOutput, Func<string, IStreamProcess> streamProcessFactoryHandler, IStreamContextCache cache) : base(transportOutput, streamProcessFactoryHandler, cache)
        {
        }

        /// <summary>
        /// Attempts to retrieve the streamId from the package according to Kafka protocol
        /// </summary>
        /// <param name="transportContext">The transport contect to retrieve the streamId from</param>
        /// <param name="streamId">The streamId retrieved</param>
        /// <returns>Whether retrieval was successful</returns>
        protected override bool TryGetStreamId(TransportContext transportContext, out string streamId)
        {
            streamId = transportContext.GetKey();
            if (streamId == null) return false;
            if (streamId.IndexOfAny(new char[] {'/', '\\'}) > -1)
            {
                streamId = streamId.Replace("/", "-").Replace("\\", "-");
            }

            return true;
        }
    }
}
