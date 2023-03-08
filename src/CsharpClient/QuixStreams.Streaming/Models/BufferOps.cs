using System;
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
        // private static ObjectPool<Dictionary<string, double?[]>> DoubleDictPool =
        //     new DefaultObjectPool<Dictionary<string, double?[]>>(
        //         new DefaultPooledObjectPolicy<Dictionary<string, double?[]>>());
        //
        // private static ObjectPool<TimeseriesBufferData> TimeseriesBufferDataPool =
        //     new DefaultObjectPool<TimeseriesBufferData>(new DefaultPooledObjectPolicy<TimeseriesBufferData>());

        private static void AddToParameterList(TimeseriesBufferDataValue[] parameterList,
            string parameterName,
            object? parameterValue,
            ParameterType parameterType,
            long epoch,
            int index)
        {
            var parameter = new TimeseriesBufferDataValue
            {
                ParameterName = parameterName,
                ParameterType = parameterType,
                ParameterValue = parameterValue,
                Epoch = epoch
            };
            parameterList[index] = parameter;
        }

        public static void AddToBuffer(ref TimeseriesBufferData buffer, TimeseriesDataRaw rawData)
        {
            for (var i = 0; i < rawData.Timestamps.Length; i++)
            {
                AddTimestampToBuffer(ref buffer, rawData, i);
            }
        }

        public static void AddTimestampToBuffer(ref TimeseriesBufferData buffer, TimeseriesDataRaw rawData, int index)
        {
            var timestamp = rawData.Timestamps[index];
            var epoch = rawData.Epoch;
            buffer.Timestamps[index] = timestamp;
            var totalParameters = rawData.NumericValues.Count +
                                  rawData.BinaryValues.Count +
                                  rawData.StringValues.Count +
                                  rawData.TagValues.Count;

            var parameterList = new TimeseriesBufferDataValue[totalParameters];

            // var numerics = new HashSet<string>();
            var parameterIndex = 0;
            foreach (var item in rawData.NumericValues)
            {
                // numerics.Add(item.Key);
                AddToParameterList(parameterList, item.Key, item.Value[index], ParameterType.Numeric, epoch,
                    parameterIndex++);
            }

            // var strings = new HashSet<string>();
            foreach (var item in rawData.StringValues)
            {
                // strings.Add(item.Key);
                AddToParameterList(parameterList, item.Key, item.Value[index], ParameterType.String, epoch,
                    parameterIndex++);
            }

            var binaries = 0;
            foreach (var item in rawData.BinaryValues)
            {
                binaries++;
                AddToParameterList(parameterList, item.Key, item.Value[index], ParameterType.Binary, epoch,
                    parameterIndex++);
            }

            var tags = 0;
            foreach (var item in rawData.TagValues)
            {
                tags++;
                AddToParameterList(parameterList, item.Key, item.Value[index], ParameterType.Tag, epoch,
                    parameterIndex++);
            }
            //
            // buffer.Numerics ??= new HashSet<string>();
            // buffer.Strings ??= new HashSet<string>();
            // buffer.Numerics.UnionWith(numerics);
            // buffer.Strings.UnionWith(strings);
            buffer.Parameters[index] = parameterList;
        }

        public static TimeseriesDataRaw ExtractRawData(
            List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            var totalSize = loadedData.Sum(x => x.Count);
            var globalTimestamps = new long[totalSize];
            // var estimate = loadedData.First().Data.Parameters.Count;

            // var numericParams = new HashSet<string>();
            // var stringParams = new HashSet<string>();
            // foreach (var a in loadedData)
            // {
            //     numericParams.UnionWith(a.Data.Numerics);
            //     stringParams.UnionWith(a.Data.Strings);
            // }
            //
            // var numericValues = numericParams.ToDictionary(k => k, k => new double?[totalSize]);
            // var stringValues = stringParams.ToDictionary(k => k, k => new string[totalSize]);
            var numericValues = new Dictionary<string, double?[]>();
            var stringValues = new Dictionary<string, string[]>();
            var binaryValues = new Dictionary<string, byte[][]>(0);
            var tagValues = new Dictionary<string, string[]>(0);

            var offset = 0;

            foreach (var (start, count, buffer) in loadedData)
            {
                try
                {
                    Array.Copy(buffer.Timestamps, start, globalTimestamps, offset, count);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }

                // Create dictionaries to store parameter values
                var index = offset;
                offset += count;
                for (var i = start; i < start + count; i++, index++)
                {
                    var parameters = buffer.Parameters[i];
                    foreach (var parameter in parameters)
                    {
                        var parameterName = parameter.ParameterName;
                        var parameterValue = parameter.ParameterValue;
                        try
                        {
                            switch (parameter.ParameterType)
                            {
                                case ParameterType.Numeric:
                                    // numericValues[parameterName][index] = (double?)parameterValue;
                                    
                                    if (!numericValues.TryGetValue(parameterName, out var numerics))
                                    {
                                        numerics = new double?[totalSize];
                                        numericValues[parameterName] = numerics;
                                    }

                                    numerics[index] = (double?)parameterValue;
                                    break;
                                case ParameterType.String:
                                    // stringValues[parameterName][index] = (string)parameterValue;
                                    
                                    if (!stringValues.TryGetValue(parameterName, out var strings))
                                    {
                                        strings = new string[totalSize];
                                        stringValues[parameterName] = strings;
                                    }

                                    strings[index] = (string)parameterValue;
                                    break;
                                case ParameterType.Binary:
                                    if (!binaryValues.TryGetValue(parameterName, out var binaries))
                                    {
                                        binaries = new byte[totalSize][];
                                        binaryValues[parameterName] = binaries;
                                    }

                                    binaries[index] = (byte[])parameterValue;
                                    break;
                                case ParameterType.Tag:
                                    if (!tagValues.TryGetValue(parameterName, out var tags))
                                    {
                                        tags = new string[totalSize];
                                        tagValues[parameterName] = tags;
                                    }

                                    tags[index] = (string)parameterValue;
                                    break;
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    }
                }
            }

            return new TimeseriesDataRaw(
                0,
                globalTimestamps,
                numericValues,
                stringValues,
                binaryValues,
                tagValues);
        }


        public static (int Start, int Count) MergeIndices(
            List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            if (loadedData == null || loadedData.Count == 0)
            {
                throw new ArgumentException("loadedData must contain at least one element");
            }

            // Sort the list of indices by start value
            loadedData.Sort((x, y) => x.Start.CompareTo(y.Start));

            // Merge consecutive indices
            var mergedIndices = new List<(int Start, int Count)>();
            var currentStart = loadedData[0].Start;
            var currentCount = loadedData[0].Count;
            for (int i = 1; i < loadedData.Count; i++)
            {
                var nextStart = loadedData[i].Start;
                var nextCount = loadedData[i].Count;
                if (nextStart == currentStart + currentCount)
                {
                    currentCount += nextCount;
                }
                else
                {
                    mergedIndices.Add((currentStart, currentCount));
                    currentStart = nextStart;
                    currentCount = nextCount;
                }
            }

            mergedIndices.Add((currentStart, currentCount));

            // Check that all tuples are consecutive
            var expectedStart = mergedIndices[0].Start;
            var expectedEnd = expectedStart + mergedIndices[0].Count;
            for (int i = 1; i < mergedIndices.Count; i++)
            {
                var tuple = mergedIndices[i];
                if (tuple.Start != expectedEnd)
                {
                    throw new InvalidOperationException("Not all tuples are consecutive");
                }

                expectedEnd += tuple.Count;
            }

            // Return the merged indices
            return mergedIndices[0];
        }

        public static void Release(List<(int Start, int Count, TimeseriesBufferData Data)> loadedData)
        {
            // foreach ((_, _, TimeseriesBufferData data) in loadedData)
            // {
            //     TimeseriesBufferDataPool.Return(data);
            // }
        }

        public static TimeseriesBufferData Get()
        {
            return new TimeseriesBufferData();
        }
    }

    public struct TimeseriesBufferData
    {
        public long[] Timestamps { get; set; }
        public TimeseriesBufferDataValue[][] Parameters { get; set; }
        public HashSet<string> Numerics { get; set; }
        public HashSet<string> Strings { get; set; }
    }

    public struct TimeseriesBufferDataValue
    {
        public long Epoch { get; set; }
        public string ParameterName { get; set; }
        public ParameterType ParameterType { get; set; }
        public object? ParameterValue { get; set; }
    }

    public enum ParameterType
    {
        Numeric,
        String,
        Tag,
        Binary
    }
}