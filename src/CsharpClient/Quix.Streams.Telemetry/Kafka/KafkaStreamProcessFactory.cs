using System;
using System.Text;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;

namespace Quix.Streams.Telemetry.Kafka
{
    /// <summary>
    /// <see cref="StreamProcessFactory"/> implemented for Kafka.
    /// It takes the StreamId from Message Key instead of a Metadata parameter from the default implementation
    /// </summary>
    internal class KafkaStreamProcessFactory : StreamProcessFactory
    {
        public KafkaStreamProcessFactory(IConsumer transportConsumer, Func<string, IStreamProcess> streamProcessFactoryHandler, IStreamContextCache cache) : base(transportConsumer, streamProcessFactoryHandler, cache)
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
            streamId = Encoding.UTF8.GetString(transportContext.GetKey());
            if (streamId == null) return false;
            if (streamId.IndexOfAny(new char[] {'/', '\\'}) > -1)
            {
                streamId = streamId.Replace("/", "-").Replace("\\", "-");
            }

            return true;
        }
    }
}
