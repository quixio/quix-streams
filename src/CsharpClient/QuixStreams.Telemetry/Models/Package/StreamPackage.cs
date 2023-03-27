﻿using System;
using Newtonsoft.Json;
using QuixStreams.Transport.IO;

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
        internal StreamPackage(Package transportPackage)
        {
            this.Type = transportPackage.Type;
            this.Value = transportPackage.Value;
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
            this.Value = value;
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
        public object Value { get; set; }

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