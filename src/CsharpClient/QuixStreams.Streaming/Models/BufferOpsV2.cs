using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models
{
    public class BufferOps
    {
        private static readonly ObjectPool<TimeseriesBufferDataValue> _parameterListPool =
            new DefaultObjectPool<TimeseriesBufferDataValue>(new DefaultPooledObjectPolicy<TimeseriesBufferDataValue>());

        public static void AddTimestampToBuffer(ref TimeseriesBufferData buffer, TimeseriesDataRaw rawData,
            int index)
        {
            var timestamp = rawData.Timestamps[index];
            var epoch = rawData.Epoch;
            buffer.Timestamps.Span[index] = timestamp;
            
            var totalParameters = rawData.NumericValues.Count +
                                  rawData.BinaryValues.Count +
                                  rawData.StringValues.Count +
                                  rawData.TagValues.Count;

            var parameterList = new TimeseriesBufferDataValue[totalParameters];

            var parameterIndex = 0;


            foreach (var item in rawData.NumericValues)
            {
                var parameter = new TimeseriesBufferDataValue(epoch, item.Key, item.Value[index]);
                
                // var parameter = _parameterListPool.Get();
                // parameter.Set(epoch, item.Key, item.Value[index]);
                parameterList[parameterIndex++] = parameter;
            }

            foreach (var item in rawData.StringValues)
            {
                var parameter = new TimeseriesBufferDataValue(epoch, item.Key, item.Value[index]);
                
                // var parameter = _parameterListPool.Get();
                // parameter.Set(epoch, item.Key, item.Value[index]);
                parameterList[parameterIndex++] = parameter;
            }

            foreach (var item in rawData.BinaryValues)
            {
                var parameter = new TimeseriesBufferDataValue(epoch, item.Key, item.Value[index]);
                
                // var parameter = _parameterListPool.Get();
                // parameter.Set(epoch, item.Key, item.Value[index]);
                parameterList[parameterIndex++] = parameter;
            }

            foreach (var item in rawData.TagValues)
            {
                var parameter = new TimeseriesBufferDataValue(epoch, item.Key, item.Value[index], true);
                // var parameter = _parameterListPool.Get();
                // parameter.Set(epoch, item.Key, item.Value[index], true);
                parameterList[parameterIndex++] = parameter;
            }

            buffer.Parameters.Span[index] = parameterList;
        }

      

        public static TimeseriesDataRaw ExtractRawData(
            List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            var totalSize = loadedData.Sum(x => x.Count);
            var globalTimestamps = new Memory<long>(new long[totalSize]);

            var numericValues = new Dictionary<string, double?[]>();
            var stringValues = new Dictionary<string, string[]>();
            var binaryValues = new Dictionary<string, byte[][]>();
            var tagValues = new Dictionary<string, string[]>();

            var offset = 0;

            // foreach (var (start, count, buffer) in loadedData)
            // {
            //     buffer.Timestamps.Slice(start, count).CopyTo(globalTimestamps.Slice(offset));
            //
            //     // Create dictionaries to store parameter values
            //     var index = offset;
            //     offset += count;
            //     for (var i = start; i < start + count; i++, index++)
            //     {
            //         var parameters = buffer.Parameters.Span[i];
            //         foreach (var parameter in parameters)
            //         {
            //             var parameterName = parameter.ParameterName;
            //
            //             switch (parameter.ParameterType)
            //             {
            //                 case ParameterType.Numeric:
            //
            //                     if (!numericValues.TryGetValue(parameterName, out var numerics))
            //                     {
            //                         numerics = new double?[totalSize];
            //                         numericValues[parameterName] = numerics;
            //                     }
            //
            //                     numerics[index] = parameter.NumericValue;
            //                     break;
            //                 case ParameterType.String:
            //
            //                     if (!stringValues.TryGetValue(parameterName, out var strings))
            //                     {
            //                         strings = new string[totalSize];
            //                         stringValues[parameterName] = strings;
            //                     }
            //
            //                     strings[index] = parameter.StringValue;
            //                     break;
            //                 case ParameterType.Binary:
            //                     if (!binaryValues.TryGetValue(parameterName, out var binaries))
            //                     {
            //                         binaries = new byte[totalSize][];
            //                         binaryValues[parameterName] = binaries;
            //                     }
            //
            //                     binaries[index] = parameter.BinaryValue;
            //                     break;
            //                 case ParameterType.Tag:
            //                     if (!tagValues.TryGetValue(parameterName, out var tags))
            //                     {
            //                         tags = new string[totalSize];
            //                         tagValues[parameterName] = tags;
            //                     }
            //
            //                     tags[index] = parameter.StringValue;
            //                     break;
            //             }
            //         }
            //     }
            // }

            return new TimeseriesDataRaw(
                0,
                globalTimestamps.ToArray(),
                numericValues,
                stringValues,
                binaryValues,
                tagValues);
        }

        public static void Release(List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            // Task.Factory.StartNew(() =>
            // {
                // Parallel.ForEach(loadedData, data =>
                // {
                //     for (var i = 0; i < data.Data.Parameters.Length; i++)
                //     {
                //         for (var j = 0; j < data.Data.Parameters.Length; j++)
                //         {
                //             _parameterListPool.Return(data.Data.Parameters.Span[i][j]);
                //         }
                //     }
                // });
            // });
        }

        public static TimeseriesBufferData Get()
        {
            return new TimeseriesBufferData();
        }
    }

    public struct TimeseriesBufferData
    {
        public Memory<long> Timestamps;
        public Memory<TimeseriesBufferDataValue[]> Parameters;
        public int TotalParameters;
    }

    public class TimeseriesBufferDataValue
    {
        public long Epoch;
        public string ParameterName;
        public ParameterType ParameterType;
        public double? NumericValue;
        public string StringValue;
        public byte[] BinaryValue;

        public TimeseriesBufferDataValue()
        {
            
        }
        
        public TimeseriesBufferDataValue(long epoch, string parameterName, string value, bool isTag = false)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = value;
            NumericValue = null;
            BinaryValue = null;
            ParameterType = isTag ? ParameterType.Tag : ParameterType.String;
        }
        public TimeseriesBufferDataValue(long epoch, string parameterName, double? value)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = null;
            NumericValue = value;
            BinaryValue = null;
            ParameterType = ParameterType.Numeric;
        }
        public TimeseriesBufferDataValue(long epoch, string parameterName, byte[] value)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = null;
            NumericValue = null;
            BinaryValue = value;
            ParameterType = ParameterType.Binary;
        }
        public void Set(long epoch, string parameterName, string value, bool isTag = false)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = value;
            NumericValue = null;
            BinaryValue = null;
            ParameterType = isTag ? ParameterType.Tag : ParameterType.String;
        }
        public void Set(long epoch, string parameterName, double? value)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = null;
            NumericValue = value;
            BinaryValue = null;
            ParameterType = ParameterType.Numeric;
        }
        public void Set(long epoch, string parameterName, byte[] value)
        {
            Epoch = epoch;
            ParameterName = parameterName;
            StringValue = null;
            NumericValue = null;
            BinaryValue = value;
            ParameterType = ParameterType.Binary;
        }
    }

    public enum ParameterType
    {
        Numeric,
        String,
        Tag,
        Binary
    }
}