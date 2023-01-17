using System.Collections.Generic;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Builder for managing <see cref="ParameterDataTimestamp"/> instances on <see cref="ParametersBufferWriter"/> 
    /// </summary>
    public class ParameterDataBuilder
    {
        private readonly ParametersBufferWriter buffer;
        private readonly ParameterData data;
        private readonly ParameterDataTimestamp timestamp;

        /// <summary>
        /// Initializes a new instance of <see cref="ParameterDataBuilder"/>
        /// </summary>
        /// <param name="buffer">The <see cref="ParametersBufferWriter"/> to adds to</param>
        /// <param name="data">Instance of ParameterData to modify</param>
        /// <param name="timestamp">Data package managed by the builder</param>
        public ParameterDataBuilder(ParametersBufferWriter buffer, ParameterData data, ParameterDataTimestamp timestamp)
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
        public ParameterDataBuilder AddValue(string parameterId, double value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }

        /// <summary>
        /// Adds new parameter value at the time the builder is created for
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">String value</param>
        public ParameterDataBuilder AddValue(string parameterId, string value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }
        
        /// <summary>
        /// Adds new parameter value at the time the builder is created for
        /// </summary>
        /// <param name="parameterId">Parameter Id</param>
        /// <param name="value">Binary value</param>
        public ParameterDataBuilder AddValue(string parameterId, byte[] value)
        {
            this.timestamp.AddValue(parameterId, value);

            return this;
        }
        

        /// <summary>
        /// Adds a tag to the values.
        /// </summary>
        /// <param name="tagId">Tag Id</param>
        /// <param name="value">Tag value</param>
        public ParameterDataBuilder AddTag(string tagId, string value)
        {
            this.timestamp.AddTag(tagId, value);

            return this;
        }
        
        /// <summary>
        /// Adds tags to the values.
        /// </summary>
        /// <param name="tags">Tags.</param>
        public ParameterDataBuilder AddTags(IEnumerable<KeyValuePair<string, string>> tags)
        {
            this.timestamp.AddTags(tags);

            return this;
        }

        /// <summary>
        /// Write the values
        /// </summary>
        public void Write()
        {
            this.buffer.Write(data);
        }

    }

}
