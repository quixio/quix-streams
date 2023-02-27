using System.Collections.Generic;

namespace Quix.Streams.Transport.IO
{
    /// <summary>
    /// Context holder for package when transporting through the pipeline
    /// </summary>
    public class TransportContext : Dictionary<string, object>
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TransportContext" /> using the provided dictionary
        /// </summary>
        /// <param name="dictionary">The dictionary to use as base</param>
        public TransportContext(IDictionary<string, object> dictionary) : base(dictionary ?? new Dictionary<string, object>())
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="TransportContext" /> that is empty
        /// </summary>
        public TransportContext() : base(new Dictionary<string, object>())
        {
        }

        /// <summary>
        /// Get the value converted to the type associated to the key
        /// </summary>
        /// <param name="key">The key</param>
        /// <param name="obj">The value</param>
        /// <typeparam name="T"></typeparam>
        /// <returns>True if found and of type, else false</returns>
        public bool TryGetTypedValue<T>(string key, out T obj)
        {
            if (!base.TryGetValue(key, out var myobj))
            {
                obj = default(T);
                return false;
            }

            if (myobj is T asType)
            {
                obj = asType;
                return true;
            }

            obj = default(T);
            return false;
        }
    }

    /// <summary>
    /// Well known transport context keys
    /// </summary>
    public static class KnownTransportContextKeys
    {
        /// <summary>
        /// The unique identifier, which groups together messages.
        /// Type is <see cref="string" />
        /// </summary>
        public const string MessageGroupKey = "MessageGroupKey";
    }
}