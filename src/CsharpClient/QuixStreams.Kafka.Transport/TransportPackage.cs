using System;

namespace QuixStreams.Kafka.Transport
{
    /// <summary>
    /// TransportPackage holds the content value and its metadata with extra context relevant for transporting it.
    /// </summary>
    /// <typeparam name="TContent">The type of the content</typeparam>
    public sealed class TransportPackage<TContent> : TransportPackage
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TransportPackage{TContent}"/>
        /// </summary>
        /// <param name="key">The key of the package. Can be null</param>
        /// <param name="value">The lazy content value</param>
        /// <param name="kafkaMessage">The optional message the package is derived from</param>
        public TransportPackage(string key, TContent value, KafkaMessage kafkaMessage = null) : base(typeof(TContent), key, value, kafkaMessage)
        {
            this.Value = value;
        }

        /// <summary>
        /// The content value of the transportPackage
        /// </summary>
        public new TContent Value { get; }
    }

    /// <summary>
    /// TransportPackage holds a value and its metadata with extra context relevant for transporting it.
    /// </summary>
    public class TransportPackage
    {
        /// <summary>
        /// Here for JSON deserialization
        /// </summary>
        protected TransportPackage()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportPackage"/>
        /// </summary>
        /// <param name="type">The type of the content value</param>
        /// <param name="key">The key of the package. Can be null</param>
        /// <param name="value">The value of the package.</param>
        /// <param name="kafkaMessage">The optional message the package is derived from</param>
        public TransportPackage(Type type, string key, object value, KafkaMessage kafkaMessage = null)
        {
            this.Type = type;
            this.Key = key;
            this.Value = value;
            this.KafkaMessage = kafkaMessage;
        }
        
        
        /// <summary>
        /// The key of the package. Can be null.
        /// </summary>
        public string Key { get; protected set; }

        /// <summary>
        /// The value of the package
        /// </summary>
        public object Value { get; }

        /// <summary>
        /// The type of the value
        /// </summary>
        public Type Type { get; }
        
        /// <summary>
        /// The kafka message this package is derived from. Will be null unless message comes from broker.
        /// </summary>
        public KafkaMessage KafkaMessage { get; }


        /// <summary>
        /// Converts the TransportPackage to type provided if possible
        /// </summary>
        /// <typeparam name="TContent">The type of the content</typeparam>
        /// <returns>Null if failed to convert else the typed QuixStreams.Kafka.Transport.Fw.TransportPackage</returns>
        public virtual bool TryConvertTo<TContent>(out TransportPackage<TContent> convertedTransportPackage)
        {
            if (this is TransportPackage<TContent> pack)
            {
                convertedTransportPackage = pack;
                return true;
            }

            if (!typeof(TContent).IsAssignableFrom(this.Type))
            {
                convertedTransportPackage = null;
                return false;
            }

            convertedTransportPackage = new TransportPackage<TContent>(this.Key, (TContent)this.Value, this.KafkaMessage);
            return true;
        }
    }
}