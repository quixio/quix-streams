using System;
using Newtonsoft.Json;
using Quix.Streams.Transport.IO;

namespace Quix.Streams.Telemetry.Models
{
    /// <summary>
    /// Default model implementation for non-typed message packages of the Process layer. It holds a value and its type.
    /// </summary>
    public class StreamPackage
    {
        private object value = null;
        private readonly Lazy<object> lazyValue;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPackage"/>
        /// </summary>
        /// <param name="transportPackage">The source transport package</param>
        internal StreamPackage(Package transportPackage)
        {
            this.Type = transportPackage.Type;
            this.lazyValue = transportPackage.Value;
            this.TransportContext = transportPackage.TransportContext;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPackage"/>
        /// </summary>
        /// <param name="type">The type of the content value</param>
        /// <param name="value">The content value of the package</param>
        public StreamPackage(Type type, object value)
        {
            this.Type = type;
            this.value = value;
            this.TransportContext = new TransportContext();
        }

        /// <summary>
        /// Type of the content value
        /// </summary>
        public Type Type { get; set; }
        
        /// <summary>
        /// Context holder for package when transporting through the pipeline
        /// </summary>
        public TransportContext TransportContext { get; set; }

        /// <summary>
        /// Content value of the package
        /// </summary>
        public object Value
        {
            get => this.value ?? this.lazyValue?.Value;
            set => this.value = value;
        }

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