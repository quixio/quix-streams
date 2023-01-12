using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Quix.Sdk.Transport.Codec;

namespace Quix.Sdk.Process.Models.Codecs
{
    /// <summary>
    /// ParameterData Protobuf Codec implementation
    /// </summary>
    public class ParameterDataProtobufCodec : Codec<ParameterDataRaw>
    {
        /// <inheritdoc />
        public override CodecId Id => CodecId.WellKnownCodecIds.ProtobufCodec+"-Pd";

        private Dictionary<string, string[]> DeserializeStrings(RepeatedField<ParameterDataRawProto.Types.StringValues> inp)
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

        private Dictionary<string, double?[]> DeserializeNumerics(RepeatedField<ParameterDataRawProto.Types.NumericValues> inp)
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

        private Dictionary<string, byte[][]> DeserializeBinaries(RepeatedField<ParameterDataRawProto.Types.BinaryValues> inp)
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
        
        private void SerializeStrings(Dictionary<string, string[]> inp, RepeatedField<ParameterDataRawProto.Types.StringValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new ParameterDataRawProto.Types.StringValues()
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

        private void SerializeNumerics(Dictionary<string, double?[]> inp, RepeatedField<ParameterDataRawProto.Types.NumericValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new ParameterDataRawProto.Types.NumericValues()
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
        
        private void SerializeBinaries(Dictionary<string, byte[][]> inp, RepeatedField<ParameterDataRawProto.Types.BinaryValues> ret)
        {
            if (inp == null)
                return;
            foreach (var keyValuePair in inp)
            {
                var row = new ParameterDataRawProto.Types.BinaryValues()
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
        public override ParameterDataRaw Deserialize(byte[] contentBytes)
        {
            var raw = ParameterDataRawProto.Parser.ParseFrom(ByteString.CopyFrom(contentBytes));

            var NumericValues = DeserializeNumerics(raw.NumericValues);
            var StringValues = DeserializeStrings(raw.StringValues);
            var BinaryValues = DeserializeBinaries(raw.BinaryValues);
            var TagValues = DeserializeStrings(raw.TagValues);
            var ret = new ParameterDataRaw
            {
                Epoch = raw.Epoch,
                Timestamps = raw.Timestamps.ToArray(),
                NumericValues = NumericValues,
                StringValues = StringValues,
                BinaryValues = BinaryValues,
                TagValues = TagValues,
            };
            return ret;
        }
        
        /// <inheritdoc />
        public override byte[] Serialize(ParameterDataRaw obj)
        {
            using (MemoryStream memStream = new MemoryStream())
            {
                byte[] ret;
                var cl = new ParameterDataRawProto
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