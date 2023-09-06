using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using QuixStreams.Kafka.Transport.SerDes.Codecs;

namespace QuixStreams.Telemetry.Models.Codecs
{
    /// <summary>
    /// TimeseriesData Protobuf Codec implementation
    /// </summary>
    public class TimeseriesDataProtobufCodec : Codec<TimeseriesDataRaw>
    {
        /// <inheritdoc />
        public override CodecId Id => CodecId.WellKnownCodecIds.ProtobufCodec+"-Pd"; // Used to be called ParameterData. Keeping it for time being to be backward compatible

        private Dictionary<string, string[]> DeserializeStrings(RepeatedField<TimeseriesDataRawProto.Types.StringValues> inp)
        {
            var ret = new Dictionary<string, string[]>(inp.Count);
            for (var i = 0; i < inp.Count; i++)
            {
                var curfield = inp[i];
                string key = curfield.Header;

                var len = curfield.Values.Count;
                
                var isFieldNull = new bool[len];
                curfield.Isnull.CopyTo(isFieldNull, 0);

                var fieldValues = new string[len];
                curfield.Values.CopyTo(fieldValues, 0);

                string[] values = new string[len];
                for (var j = 0; j < len; ++j)
                {
                    values[j] = isFieldNull[j] ? null : fieldValues[j];
                }

                ret.Add(key, values);
            }
            return ret;
        }

        private Dictionary<string, double?[]> DeserializeNumerics(RepeatedField<TimeseriesDataRawProto.Types.NumericValues> inp)
        {
            var ret = new Dictionary<string, double?[]>(inp.Count);
            for (var i = 0; i < inp.Count; i++)
            {
                var curfield = inp[i];
                string key = curfield.Header;

                var len = curfield.Values.Count;

                var isFieldNull = new bool[len];
                curfield.Isnull.CopyTo(isFieldNull, 0);

                var fieldValues = new double[len];
                curfield.Values.CopyTo(fieldValues, 0);

                var values = new double?[len];
                for (var j = 0; j < len; ++j)
                {
                    values[j] = isFieldNull[j] ? null : (double?)fieldValues[j];
                }
                ret.Add(key, values);
            }
            return ret;
        }

        private Dictionary<string, byte[][]> DeserializeBinaries(RepeatedField<TimeseriesDataRawProto.Types.BinaryValues> inp)
        {
            var ret = new Dictionary<string, byte[][]>(inp.Count);
            for (var i = 0; i < inp.Count; i++)
            {
                var curfield = inp[i];
                string key = curfield.Header;

                var len = curfield.Values.Count;

                var isFieldNull = new bool[len];
                curfield.Isnull.CopyTo(isFieldNull, 0);

                var fieldValues = new ByteString[len];
                curfield.Values.CopyTo(fieldValues, 0);

                
                byte[][] values = new byte[len][];
                for (var j = 0; j < len; ++j)
                {
                    if (isFieldNull[j])
                    {
                        values[j] = null;
                    }
                    else
                    {
                        values[j] = fieldValues[j].ToByteArray();
                    }
                }
                ret.Add(key, values);
            }
            return ret;
        }
        
        private void SerializeStrings(Dictionary<string, string[]> inp, RepeatedField<TimeseriesDataRawProto.Types.StringValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new TimeseriesDataRawProto.Types.StringValues()
                {
                    Header = keyValuePair.Key
                };


                string[] strings = new string[keyValuePair.Value.Length];
                bool[] isNulls = new bool[keyValuePair.Value.Length];

                int len = keyValuePair.Value.Length;
                for (var i = 0; i < len; ++i)
                {
                    var isnull = keyValuePair.Value[i] == null;
                    var value = isnull ? "" : (string) keyValuePair.Value[i];

                    isNulls[i] = isnull;
                    strings[i] = value;
                }
                
                row.Isnull.AddRange(isNulls);
                row.Values.AddRange(strings);

                ret.Add(row);
            }

        }

        private void SerializeNumerics(Dictionary<string, double?[]> inp, RepeatedField<TimeseriesDataRawProto.Types.NumericValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new TimeseriesDataRawProto.Types.NumericValues()
                {
                    Header = keyValuePair.Key
                };

                int len = keyValuePair.Value.Length;
                double[] values = new double[len];
                bool[] isNulls = new bool[len];
                for (var i = 0; i < len; ++i)
                {
                    var isnull = keyValuePair.Value[i] == null;
                    var value = isnull ? 0 : (double)keyValuePair.Value[i];

                    isNulls[i] = isnull;
                    values[i] = value;
                }
                row.Isnull.AddRange(isNulls);
                row.Values.AddRange(values);
                
                ret.Add(row);
            }
        }
        
        private void SerializeBinaries(Dictionary<string, byte[][]> inp, RepeatedField<TimeseriesDataRawProto.Types.BinaryValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new TimeseriesDataRawProto.Types.BinaryValues()
                {
                    Header = keyValuePair.Key
                };

                int len = keyValuePair.Value.Length;
                ByteString[] values = new ByteString[len];
                bool[] isNulls = new bool[len];
                for (var i = 0; i < len; ++i)
                {
                    var isnull = keyValuePair.Value[i] == null;
                    var value = isnull ? new byte[0] : keyValuePair.Value[i];

                    isNulls[i] = isnull;
                    values[i] = ByteString.CopyFrom(value);
                }
                row.Isnull.AddRange(isNulls);
                row.Values.AddRange(values);
                
                ret.Add(row);
            }
        }

        /// <inheritdoc />
        public override TimeseriesDataRaw Deserialize(byte[] contentBytes)
        {
            var raw = TimeseriesDataRawProto.Parser.ParseFrom(ByteString.CopyFrom(contentBytes));

            var numericValues = DeserializeNumerics(raw.NumericValues);
            var stringValues = DeserializeStrings(raw.StringValues);
            var binaryValues = DeserializeBinaries(raw.BinaryValues);
            var tagValues = DeserializeStrings(raw.TagValues);
            var ret = new TimeseriesDataRaw
            {
                Epoch = raw.Epoch,
                Timestamps = raw.Timestamps.ToArray(),
                NumericValues = numericValues,
                StringValues = stringValues,
                BinaryValues = binaryValues,
                TagValues = tagValues,
            };
            return ret;
        }

        /// <inheritdoc />
        public override TimeseriesDataRaw Deserialize(ArraySegment<byte> contentBytes)
        {
            var raw = TimeseriesDataRawProto.Parser.ParseFrom(ByteString.CopyFrom(contentBytes.Array, contentBytes.Offset, contentBytes.Count));

            var numericValues = DeserializeNumerics(raw.NumericValues);
            var stringValues = DeserializeStrings(raw.StringValues);
            var binaryValues = DeserializeBinaries(raw.BinaryValues);
            var tagValues = DeserializeStrings(raw.TagValues);
            var ret = new TimeseriesDataRaw
            {
                Epoch = raw.Epoch,
                Timestamps = raw.Timestamps.ToArray(),
                NumericValues = numericValues,
                StringValues = stringValues,
                BinaryValues = binaryValues,
                TagValues = tagValues,
            };
            return ret;
        }

        /// <inheritdoc />
        public override byte[] Serialize(TimeseriesDataRaw obj)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                byte[] ret;
                var cl = new TimeseriesDataRawProto
                {
                    Epoch = obj.Epoch,
                };
                cl.Timestamps.AddRange(obj.Timestamps);
                SerializeBinaries(obj.BinaryValues, cl.BinaryValues);
                SerializeStrings(obj.StringValues, cl.StringValues);
                SerializeStrings(obj.TagValues, cl.TagValues);
                SerializeNumerics(obj.NumericValues, cl.NumericValues);
                cl.WriteTo(memStream);
                memStream.Flush();
                ret = memStream.ToArray();
                return ret;
            }
        }
        
    }
}