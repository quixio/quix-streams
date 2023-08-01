using System;
using System.IO;
using System.Linq;
using QuixStreams.State.Storage.FileStorage;

namespace QuixStreams.State.Serializers
{
    /// <summary>
    /// Serializer written using the BinaryWriter and BinaryReader
    /// Custom protocol
    /// Implements crc32 checksum to check the validity of stored data
    /// </summary>
    public class ByteValueSerializer
    {
        private const char CodecId = 'b';
        private const char TypeBool = 'b';
        private const char TypeLong = 'l';
        private const char TypeDouble = 'd';
        private const char TypeBinary = 'a';
        private const char TypeObject = 'o';
        private const char TypeString = 's';

        private const int CrcChecksumLength = 32 / 8; // CRC hash 32 bit long, so 4 bytes

        private static byte[] calculateCRC32Hash(byte[] data)
        {
            Crc32 crc32 = new Crc32();
            return crc32.ComputeHash(data);
        }

        /// <summary>
        /// Serialize State value
        /// </summary>
        /// <param name="value">Value to serialize</param>
        /// <returns>Serialized byte array</returns>
        public static byte[] Serialize(StateValue value)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(CodecId);
                    switch (value.Type)
                    {
                        case StateValue.StateType.Binary:
                            {
                                byte[] data = value.BinaryValue;
                                writer.Write(TypeBinary);

                                if (data != null)
                                {
                                    writer.Write(data.Length);
                                    writer.Write(data);
                                }
                                else
                                {
                                    writer.Write(-1);
                                }
                            }
                            break;
                        case StateValue.StateType.Object:
                        {
                            byte[] data = value.BinaryValue ?? Array.Empty<byte>();
                            writer.Write(TypeObject);
                            if (data != null)
                            {
                                writer.Write(data.Length);
                                writer.Write(data);
                            }
                            else
                            {
                                writer.Write(-1);
                            }
                        }
                            break;                        
                        case StateValue.StateType.Bool:
                            {
                                bool data = value.BoolValue;
                                writer.Write(TypeBool);
                                writer.Write(data);
                            }
                            break;
                        case StateValue.StateType.Double:
                            {
                                double data = value.DoubleValue;
                                writer.Write(TypeDouble);
                                writer.Write(data);
                            }
                            break;
                        case StateValue.StateType.Long:
                            {
                                long data = value.LongValue;
                                writer.Write(TypeLong);
                                writer.Write(data);
                            }
                            break;
                        case StateValue.StateType.String:
                            {
                                string data = value.StringValue ?? ""; // null would throw arg exc
                                writer.Write(TypeString);
                                writer.Write(data);
                            }
                            break;
                        default:
                            throw new NotSupportedException($"Unsupported type {value.Type} in Serialization");
                    }
                    //compute crc32 hash to check the validity and append it at the end
                    var raw = stream.ToArray();
                    byte[] hash = calculateCRC32Hash(raw);
                    writer.Write(hash);
                }
                stream.Flush();
                return stream.ToArray();
            }
        }

        private static void ValidateChecksum(byte[] data)
        {
            //check for crc32
            if (data.Length <= CrcChecksumLength)
            {
                throw new FormatException("Malformed data file");
            }
            var newData = new byte[data.Length - CrcChecksumLength];
            Array.Copy(data, 0, newData, 0, newData.Length);
            var checksum = new byte[CrcChecksumLength];
            Array.Copy(data, newData.Length, checksum, 0, CrcChecksumLength);
            
            byte[] computedHash = calculateCRC32Hash(newData); 
            if (!computedHash.SequenceEqual(checksum))
            {
                throw new FormatException("Malformed data file");
            }

        }

        /// <summary>
        /// Deserialize State value
        /// </summary>
        /// <param name="data">Byte array to deserialize into a State value</param>
        /// <returns>Deserialized State value</returns>
        public static StateValue Deserialize(byte[] data)
        {
            if (data == null)
                return null;
            
            ValidateChecksum(data);

            using (MemoryStream stream = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    char codecID = reader.ReadChar();
                    if(codecID != CodecId)
                    {
                        throw new NotSupportedException($"Wrong codec id");
                    }
                    char type = reader.ReadChar();
                    switch (type)
                    {
                        case TypeBinary:
                            {
                                int dataLength = reader.ReadInt32();
                                if (dataLength == -1) return new StateValue((byte[])null);
                                return new StateValue(reader.ReadBytes(dataLength));
                            }
                        case TypeObject:
                            {
                                int dataLength = reader.ReadInt32();
                                if (dataLength == -1) return new StateValue((byte[])null);
                                return new StateValue(reader.ReadBytes(dataLength), StateValue.StateType.Object);
                            }                        
                        case TypeBool:
                            {
                                return new StateValue(reader.ReadBoolean());
                            }
                        case TypeDouble:
                            {
                                return new StateValue(reader.ReadDouble());
                            }
                        case TypeLong:
                            {
                                return new StateValue(reader.ReadInt64());
                            }
                        case TypeString:
                            {
                                return new StateValue(reader.ReadString());
                            }
                        default:
                            throw new NotSupportedException($"Unsupported type {type} in Serialization");
                    }
                }
            }

        }

    }
}
