using System;
using System.Text;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Kafka;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>
    /// <see cref="StreamPipelineFactory"/> implemented for Kafka.
    /// It takes the StreamId from Message Key instead of a Metadata parameter from the default implementation
    /// </summary>
    internal class KafkaStreamPipelineFactory : StreamPipelineFactory
    {
        public KafkaStreamPipelineFactory(IConsumer transportConsumer, Func<string, IStreamPipeline> streamPipelineFactoryHandler, IStreamContextCache cache) : base(transportConsumer, streamPipelineFactoryHandler, cache)
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
