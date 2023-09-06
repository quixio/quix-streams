using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// Codec registry. If multiple codecs are registered for the same ModelKey, the last one registered will be used
    /// </summary>
    public static class CodecRegistry
    {
        /// <summary>
        /// A concurrent codec dictionary in case people use codec registration via multiple threads
        /// </summary>
        private static readonly ConcurrentDictionary<ModelKey, IReadOnlyCollection<ICodec>> Codecs = new ConcurrentDictionary<ModelKey, IReadOnlyCollection<ICodec>>();

        private static ICodec defaultCodec = DefaultJsonCodec.Instance;

        static CodecRegistry()
        {
            RegisterCodec(typeof(string), StringCodec.Instance);
            RegisterCodec(ModelKey.WellKnownModelKeys.String, StringCodec.Instance);
            RegisterCodec(typeof(byte[]), ByteCodec.Instance);
            RegisterCodec(ModelKey.WellKnownModelKeys.ByteArray, ByteCodec.Instance);
        }

        /// <summary>
        /// Registers a codec for the provided key. If multiple codecs are registered for the same ModelKey, the last one registered will be used for writing
        /// </summary>
        /// <param name="modelKey">The model key to register the codec for</param>
        /// <param name="codec">The codec to register</param>
        public static void RegisterCodec(ModelKey modelKey, ICodec codec)
        {
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));
            if (codec == null) throw new ArgumentNullException(nameof(codec));

            IReadOnlyCollection<ICodec> list = new List<ICodec>
            {
                codec
            }.AsReadOnly();

            if (Codecs.TryAdd(modelKey, list))
            {
                ModelKeyRegistry.RegisterModel(codec.Type, modelKey);
                return;
            }

            if (!Codecs.TryGetValue(modelKey, out list))
            {
                // what is going on, is some other thread trying to add to the exact same modelKey?
                // try again
                RegisterCodec(modelKey, codec);
                return;
            }

            var newList = new List<ICodec>(list.Count + 1)
            {
                codec
            };
            newList.AddRange(list.Where(x => x.Id != codec.Id)); // do not store dupes, set new copy as first
            if (!Codecs.TryUpdate(modelKey, newList, list))
            {
                // what is going on, is some other thread trying to add to the exact same modelKey?
                // try again
                RegisterCodec(modelKey, codec);
            }
            else
            {
                ModelKeyRegistry.RegisterModel(codec.Type, modelKey);
            }
        }

        /// <summary>
        /// Clears the codecs for the specified key
        /// </summary>
        /// <param name="modelKey">The model key to clear codecs for</param>
        public static void ClearCodecs(ModelKey modelKey)
        {
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));

            Codecs.TryRemove(modelKey, out _);
        }

        /// <summary>
        /// Retrieves registered codecs for the key
        /// </summary>
        /// <param name="modelKey">The model key to retrieve codecs for</param>
        /// <returns>Previously registered <see cref="ICodec"/> or an empty collection if none found</returns>
        public static IReadOnlyCollection<ICodec> RetrieveCodecs(ModelKey modelKey)
        {
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));

            if (!Codecs.TryGetValue(modelKey, out var typeCodecs))
            {
                return Array.Empty<ICodec>();
            }

            return typeCodecs;
        }

        /// <summary>
        /// Retrieves codec for the key and codec id
        /// </summary>
        /// <param name="modelKey">The model key</param>
        /// <param name="codecId">The codec Id</param>
        /// <returns><see cref="ICodec"/> if found with provided details, else null</returns>
        public static ICodec RetrieveCodec(ModelKey modelKey, CodecId codecId)
        {
            if (modelKey == null) throw new ArgumentNullException(nameof(modelKey));
            if (codecId == null) throw new ArgumentNullException(nameof(codecId));

            return RetrieveCodecs(modelKey).FirstOrDefault(x => x.Id == codecId);
        }
    }
}