using System.Collections.Generic;

namespace QuixStreams.Streaming.Models.StreamProducer
{
    /// <summary>
    /// Builder for managing <see cref="TimeseriesDataTimestamp"/> instances on <see cref="TimeseriesBufferProducer"/> 
    /// </summary>
    public class TimeseriesDataBuilder
    {
        private readonly TimeseriesBufferProducer buffer;
        private readonly TimeseriesData data;
        private readonly TimeseriesDataTimestamp timestamp;

        /// <summary>
        /// Initializes a new instance of <see cref="TimeseriesDataBuilder"/>
        /// </summary>
        /// <param name="buffer">The <see cref="TimeseriesBufferProducer"/> to adds to</param>
        /// <param name="data">Instance of TimeseriesData to modify</param>
        /// <param name="timestamp">Data package managed by the builder</param>
        public TimeseriesDataBuilder(TimeseriesBufferProducer buffer, TimeseriesData data, TimeseriesDataTimestamp timestamp)
        {
            this.buffer = buffer;
            this.data = data;
            this.timestamp = timestamp;
        }

        /// <summary>
        /// Adds new parameter value at the time the builder is created for
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Numeric value</param>
        public TimeseriesDataBuilder AddValue(string parameterId, double value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }

        /// <summary>
        /// Adds new parameter value at the time the builder is created for
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        public TimeseriesDataBuilder AddValue(string parameterId, string value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }
        
        /// <summary>
        /// Adds new parameter value at the time the builder is created for
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Binary value</param>
        public TimeseriesDataBuilder AddValue(string parameterId, byte[] value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }
        

        /// <summary>
        /// Adds a tag to the values.
        /// </summary>
        /// <param name="tagId">Tag Id</param>
        /// <param name="value">Tag value</param>
        public TimeseriesDataBuilder AddTag(string tagId, string value)
        {
            this.timestamp.AddTag(tagId, value);

            return this;
        }
        
        /// <summary>
        /// Adds tags to the values.
        /// </summary>
        /// <param name="tags">Tags to add.</param>
        public TimeseriesDataBuilder AddTags(IEnumerable<KeyValuePair<string, string>> tags)
        {
            this.timestamp.AddTags(tags);

            return this;
        }

        /// <summary>
        /// Publish the values
        /// </summary>
        public void Publish()
        {
            this.buffer.Publish(data);
        }

    }

}
