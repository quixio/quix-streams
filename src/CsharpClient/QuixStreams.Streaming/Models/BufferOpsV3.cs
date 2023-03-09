using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models
{
    public class BufferOps
    {
        public static void AddTimestampToBuffer(TimeseriesBufferData buffer, TimeseriesDataRaw rawData, int index)
        {
            if (index >= buffer.Timestamps.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
            }

            var nameIndex = 0;
            foreach (var values in rawData.NumericValues.Values)
            {
                buffer.NumericValues.Span[nameIndex++].Span[index] = values[index];
            }

            var stringIndex = 0;
            foreach (var values in rawData.StringValues.Values)
            {
                buffer.StringValues.Span[stringIndex++].Span[index] = values[index];
            }

            var binaryIndex = 0;
            foreach (var values in rawData.BinaryValues.Values)
            {
                buffer.BinaryValues.Span[binaryIndex++].Span[index] = values[index];
            }

            var tagIndex = 0;
            foreach (var values in rawData.TagValues.Values)
            {
                buffer.TagValues.Span[tagIndex++].Span[index] = values[index];
            }


            // Add the new timestamp
            buffer.Timestamps.Span[index] = rawData.Timestamps[index];
        }

        public static TimeseriesDataRaw ExtractRawData(
            List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            var result = new TimeseriesDataRaw();
            var totalTimestampsCount = loadedData.Sum(d => d.Count);

            var timestamps = new Memory<long>(new long[totalTimestampsCount]);
            var numericValues = new Dictionary<string, double?[]>();
            var stringValues = new Dictionary<string, string[]>();
            var binaryValues = new Dictionary<string, byte[][]>();
            var tagValues = new Dictionary<string, string[]>();

            var timestampIndex = 0;
            foreach (var (start, count, data) in loadedData)
            {
                if (timestampIndex + count > totalTimestampsCount)
                {
                    throw new ArgumentException("The loaded data is inconsistent.", nameof(loadedData));
                }

                data.Timestamps.Slice(start, count).CopyTo(timestamps.Slice(timestampIndex, count));
                
                var numericMemory = new Memory<double?>(new double?[totalTimestampsCount]);
                var numericParameterNames = data.ParameterNames.Span[ParameterType.Numeric].Span;
                var totalNumericParameters = numericParameterNames.Length;
                var numericDataValues = data.NumericValues.Span;
                
                for (var i = 0; i < totalNumericParameters; i++)
                {
                    var parameterName = numericParameterNames[i];
                    numericDataValues[i].Slice(start, count).CopyTo(numericMemory);
                    numericValues[parameterName] = numericMemory.ToArray();
                }

                
                var stringMemory = new Memory<string>(new string[totalTimestampsCount]);
                var stringParameterNames = data.ParameterNames.Span[ParameterType.String].Span;
                var totalStringParameters = stringParameterNames.Length;
                var stringDataValues = data.StringValues.Span;
                
                for (var i = 0; i < totalStringParameters; i++)
                {
                    var parameterName = stringParameterNames[i];
                    stringDataValues[i].Slice(start, count).CopyTo(stringMemory);
                    stringValues[parameterName] = stringMemory.ToArray();
                }

                
                var binaryMemory = new Memory<byte[]>(new byte[totalTimestampsCount][]);
                var binaryParameterNames = data.ParameterNames.Span[ParameterType.Binary].Span;
                var totalBinaryParameters = binaryParameterNames.Length;
                var binaryDataValues = data.BinaryValues.Span;
                
                for (var i = 0; i < totalBinaryParameters; i++)
                {
                    var parameterName = binaryParameterNames[i];
                    binaryDataValues[i].Slice(start, count).CopyTo(binaryMemory);
                    binaryValues[parameterName] = binaryMemory.ToArray();
                }
                
                
                var tagMemory = new Memory<string>(new string[totalTimestampsCount]);
                var tagParameterNames = data.ParameterNames.Span[ParameterType.Tag].Span;
                var totalTagParameters = tagParameterNames.Length;
                var tagDataValues = data.TagValues.Span;
                
                for (var i = 0; i < totalTagParameters; i++)
                {
                    var parameterName = tagParameterNames[i];
                    tagDataValues[i].Slice(start, count).CopyTo(tagMemory);
                    tagValues[parameterName] = tagMemory.ToArray();
                }
                
                timestampIndex += count;
            }

            result.Epoch = loadedData[0].Data.Epoch;
            result.Timestamps = timestamps.ToArray();
            result.NumericValues = numericValues;
            result.StringValues = stringValues;
            result.BinaryValues = binaryValues;
            result.TagValues = tagValues;

            return result;
        }

        public static TimeseriesBufferData Get(TimeseriesDataRaw rawData)
        {
            return new TimeseriesBufferData(rawData);
        }
    }

    public struct TimeseriesBufferData
    {
        public long Epoch;
        public Memory<long> Timestamps;
        public ReadOnlyMemory<Memory<double?>> NumericValues;
        public ReadOnlyMemory<Memory<string>> StringValues;
        public ReadOnlyMemory<Memory<byte[]>> BinaryValues;
        public ReadOnlyMemory<Memory<string>> TagValues;
        public ReadOnlyMemory<Memory<string>> ParameterNames;

        public TimeseriesBufferData(TimeseriesDataRaw rawData)
        {
            Epoch = rawData.Epoch;
            var totalLength = rawData.Timestamps.Length;
            
            Timestamps = new Memory<long>(new long[totalLength]);

            var numericValues = new Memory<Memory<double?>>(new Memory<double?>[rawData.NumericValues.Count]);
            var stringValues = new Memory<Memory<string>>(new Memory<string>[rawData.StringValues.Count]);
            var binaryValues = new Memory<Memory<byte[]>>(new Memory<byte[]>[rawData.TagValues.Count]);
            var tagValues = new Memory<Memory<string>>(new Memory<string>[rawData.TagValues.Count]);
            
            var namesMemory = new Memory<Memory<string>>(new Memory<string>[4]);
            var numericNameMemory = new Memory<string>(new string[rawData.NumericValues.Keys.Count]);
            var stringNameMemory = new Memory<string>(new string[rawData.StringValues.Keys.Count]);
            var binaryNameMemory = new Memory<string>(new string[rawData.BinaryValues.Keys.Count]);
            var tagNameMemory = new Memory<string>(new string[rawData.TagValues.Keys.Count]);
            
            namesMemory.Span[ParameterType.Numeric] = numericNameMemory;
            namesMemory.Span[ParameterType.String] = stringNameMemory;
            namesMemory.Span[ParameterType.Binary] = binaryNameMemory;
            namesMemory.Span[ParameterType.Tag] = tagNameMemory;

            var numericIndex = 0;
            foreach (var parameterName in rawData.NumericValues.Keys)
            {
                numericValues.Span[numericIndex] = new Memory<double?>(new double?[totalLength]);
                numericNameMemory.Span[numericIndex++] = parameterName;
            }
            
            var stringIndex = 0;
            foreach (var parameterName in rawData.StringValues.Keys)
            {
                stringValues.Span[stringIndex] = new Memory<string>(new string[totalLength]);
                stringNameMemory.Span[stringIndex++] = parameterName;
            }
            
            var binaryIndex = 0;
            foreach (var parameterName in rawData.BinaryValues.Keys)
            {
                binaryValues.Span[binaryIndex] = new Memory<byte[]>(new byte[totalLength][]);
                binaryNameMemory.Span[binaryIndex++] = parameterName;
            }

            var tagIndex = 0;
            foreach (var parameterName in rawData.TagValues.Keys)
            {
                tagValues.Span[tagIndex] = new Memory<string>(new string[totalLength]);
                tagNameMemory.Span[tagIndex++] = parameterName;
            }
            
            ParameterNames = namesMemory;
            NumericValues = numericValues;
            StringValues = stringValues;
            BinaryValues = binaryValues;
            TagValues = tagValues;
        }
    }

    public static class ParameterType
    {
        public static int Numeric = 0;
        public static int String = 1;
        public static int Tag = 2;
        public static int Binary = 3;
    }
}