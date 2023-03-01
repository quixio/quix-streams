using System;

namespace QuixStreams.Telemetry.Models
{
    /// <summary>
    /// Attributte to mark classes that the codecs will use for serialize/deserialize messages
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class ModelKeyAttribute : Attribute
    {
        /// <summary>
        /// Creates a new instance of <see cref="ModelKeyAttribute"/>
        /// </summary>
        /// <param name="modelKeyForWrite">The model key to use when looking up codec for write instead of the name of the class</param>
        /// <param name="modelKeysForRead">The model keys to use when looking up codec for read instead of the name of the class</param>
        public ModelKeyAttribute(string modelKeyForWrite, params string[] modelKeysForRead)
        {
            this.ModelKeyForWrite = modelKeyForWrite;
            this.ModelKeysForRead = modelKeysForRead;
        }

        /// <summary>
        /// The model keys to use when looking up codec for read instead of the name of the class
        /// </summary>
        public readonly string[] ModelKeysForRead;
        
        /// <summary>
        /// The model key to use when looking up codec for write instead of the name of the class
        /// </summary>
        public readonly string ModelKeyForWrite;
    }
}