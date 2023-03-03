using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;
using QuixStreams.Telemetry.Models.Codecs;
using QuixStreams.Transport.Codec;

namespace QuixStreams.Telemetry.Models.Telemetry.Parameters.Codecs
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
            
            var rows = inp
                .AsParallel()
                .Select(keyValuePair => new TimeseriesDataRawProto.Types.StringValues
                {
                    Header = keyValuePair.Key,
                    Values = { keyValuePair.Value.Select(v => v ?? "") },
                    Isnull = { keyValuePair.Value.Select(v => v == null) }
                });
    
            ret.AddRange(rows);

        }
        private void SerializeNumerics(Dictionary<string, double?[]> inp, RepeatedField<TimeseriesDataRawProto.Types.NumericValues> ret)
        {
            if (inp == null)
                return;
    
            var rows = inp
                .AsParallel()
                .Select(keyValuePair => new TimeseriesDataRawProto.Types.NumericValues
                {
                    Header = keyValuePair.Key,
                    Values = { keyValuePair.Value.Select(v => v.GetValueOrDefault()) },
                    Isnull = { keyValuePair.Value.Select(v => v == null) }
                });
    
            ret.AddRange(rows);
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
            var raw = TimeseriesDataRawProto.Parser.ParseFrom(contentBytes.AsSpan());

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
            var cl = new TimeseriesDataRawProto
            {
                Epoch = obj.Epoch,
            };
            cl.Timestamps.AddRange(obj.Timestamps);

            // var actions = new Action[]
            // {
            //     () => SerializeBinaries(obj.BinaryValues, cl.BinaryValues),
            //     () => SerializeStrings(obj.StringValues, cl.StringValues),
            //     () => SerializeStrings(obj.TagValues, cl.TagValues),
            //     () => SerializeNumerics(obj.NumericValues, cl.NumericValues)
            // };
            // // run serialization in parallel
            // actions.AsParallel().ForAll(a => a());
            
            SerializeBinaries(obj.BinaryValues, cl.BinaryValues);
            SerializeStrings(obj.StringValues, cl.StringValues);
            SerializeStrings(obj.TagValues, cl.TagValues);
            SerializeNumerics(obj.NumericValues, cl.NumericValues);
            var size = cl.CalculateSize();
            Span<byte> span = new byte[size];
            cl.WriteTo(span);
            
            return span.ToArray();
        }
        
    }
}