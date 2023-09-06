using System;

namespace QuixStreams.Kafka.Transport.SerDes.Codecs
{
    /// <summary>
    /// Interface for a codec capable of serializing and deserializing the <see cref="ICodec{TContent}"/> type
    /// </summary>
    /// <typeparam name="TContent">The type the codec works with</typeparam>
    public interface ICodec<TContent> : ICodec
    {
        /// <summary>
        /// Attempts to serialize the object with the codec.
        /// </summary>
        /// <param name="obj">The object to serialize</param>
        /// <returns>The serialized entry</returns>
        byte[] Serialize(TContent obj);

        /// <summary>
        /// Attempts to deserialize the byte array with the codec.
        /// </summary>
        /// <param name="contentBytes">The entry to serialize</param>
        /// <returns>The deserialized object</returns>
        TContent Deserialize(byte[] contentBytes);
        
        /// <summary>
        /// Attempts to deserialize the byte array with the codec.
        /// </summary>
        /// <param name="contentBytes">The entry to serialize</param>
        /// <returns>The deserialized object</returns>
        TContent Deserialize(ArraySegment<byte> contentBytes);
    }

    /// <summary>
    /// Interface for a codec capable of serializing and deserializing a type.
    /// The type depends on the <see cref="Type"/> property.
    /// </summary>
    public interface ICodec
    {
        /// <summary>
        /// The unique Identifier of the codec. Useful to differentiate different codecs for the same type when there are
        /// multiple defined.
        /// For example, if a type has both JSON and Protobuf, the Ids would be 'JSON' and 'Protobuf'.
        /// Note, you can, but do not need to use the type's full name. For serialization purposes it is better to use as short
        /// as possible yet unique Ids.
        /// </summary>
        CodecId Id { get; }

        /// <summary>
        /// The type returned by the codec when deserializing
        /// </summary>
        Type Type { get; }

        /// <summary>
        /// Attempts to serialize the object with the codec.
        /// </summary>
        /// <param name="obj">The object to serialize</param>
        /// <param name="serialized">The serialized entry</param>
        /// <returns><c>True</c> if successful else <c>false</c></returns>
        bool TrySerialize(object obj, out byte[] serialized);

        /// <summary>
        /// Attempts to deserialize the byte array with the codec.
        /// </summary>
        /// <param name="contentBytes">The entry to serialize</param>
        /// <param name="content">The deserialized object</param>
        /// <returns><c>True</c> if successful else <c>false</c></returns>
        bool TryDeserialize(byte[] contentBytes, out object content);
        
        /// <summary>
        /// Attempts to deserialize the byte array with the codec.
        /// </summary>
        /// <param name="contentBytes">The entry to serialize</param>
        /// <param name="content">The deserialized object</param>
        /// <returns><c>True</c> if successful else <c>false</c></returns>
        bool TryDeserialize(ArraySegment<byte> contentBytes, out object content);
    }
}