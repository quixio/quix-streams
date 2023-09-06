using System;
using Newtonsoft.Json;
using QuixStreams.Kafka;
using QuixStreams.Kafka.Transport;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Default model implementation for non-typed message packages of the Telemetry layer. It holds a value and its type.
    /// </summary>
    public class StreamPackage
    {

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPackage"/>
        /// </summary>
        /// <param name="transportPackage">The source transport package</param>
        internal StreamPackage(TransportPackage transportPackage)
        {
            this.Type = transportPackage.Type;
            this.Value = transportPackage.Value;
            this.KafkaMessage = transportPackage.KafkaMessage;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPackage"/>
        /// </summary>
        /// <param name="type">The type of the content value</param>
        /// <param name="value">The content value of the package</param>
        public StreamPackage(Type type, object value)
        {
            this.Type = type;
            this.Value = value;
        }

        /// <summary>
        /// Type of the content value
        /// </summary>
        public Type Type { get; }

        /// <summary>
        /// Content value of the package
        /// </summary>
        public object Value { get; }
        
        /// <summary>
        /// The Kafka message this stream package derives from
        /// Can be null if not consumed from broker
        /// </summary>
        public KafkaMessage KafkaMessage { get; }

        /// <summary>
        /// Serialize the package into Json
        /// </summary>
        /// <returns></returns>
        public string ToJson()
        {
            return JsonConvert.SerializeObject(this.Value);
        }

    }
}