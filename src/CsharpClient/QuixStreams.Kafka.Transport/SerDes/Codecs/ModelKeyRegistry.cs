using System;
using System.Collections.Generic;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// Model registry
    /// </summary>
    public static class ModelKeyRegistry
    {
        /// <summary>
        /// The lock object to sync adding to multiple dictionaries in a thread safe manner
        /// </summary>
        private static readonly object dictLock = new object();

        /// <summary>
        /// A concurrent model dictionary in case people use model registration via multiple threads
        /// </summary>
        private static readonly Dictionary<Type, ModelKey> TypesToModelKey = new Dictionary<Type, ModelKey>();

        /// <summary>
        /// A concurrent model dictionary in case people use model registration via multiple threads
        /// </summary>
        private static readonly Dictionary<ModelKey, Type> ModelKeysToTypes = new Dictionary<ModelKey, Type>();

        /// <summary>
        /// Registers a type and modelkey combination. Last registration wins.
        /// </summary>
        /// <param name="modelKey">The model key to register for the type</param>
        /// <param name="type">The type to register for the model key</param>
        public static void RegisterModel(Type type, ModelKey modelKey)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));

            lock (dictLock)
            {
                TypesToModelKey[type] = modelKey;
                ModelKeysToTypes[modelKey] = type;
            }
        }

        /// <summary>
        /// Gets the type for the model key
        /// </summary>
        /// <param name="modelKey">The model key</param>
        /// <returns>The <see cref="Type"/> if found, else null</returns>
        public static Type GetType(ModelKey modelKey)
        {
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));

            if (ModelKeysToTypes.TryGetValue(modelKey, out var type)) return type;
            return null;
        }

        /// <summary>
        /// Gets the model key for the type
        /// </summary>
        /// <param name="type">The type</param>
        /// <returns>The model key if found, else <see cref="ModelKey.WellKnownModelKeys.Default"/></returns>
        public static ModelKey GetModelKey(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            if (TypesToModelKey.TryGetValue(type, out var key)) return key;
            return ModelKey.WellKnownModelKeys.Default;
        }
    }
}