// using System;
// using System.Collections.Concurrent;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;
// using QuixStreams.Telemetry.Models;
//
// namespace QuixStreams.Streaming.Models
// {
//     public class TimeseriesBufferIdea
//     {
//         private List<TimeseriesBufferData> buffer = new List<TimeseriesBufferData>();
//
//         public void WriteChunk(TimeseriesDataRaw timeseriesDataRaw)
//         {
//             for (int i = 0; i < timeseriesDataRaw.Timestamps.Length; i++)
//             {
//                 var timestamp = timeseriesDataRaw.Timestamps[i];
//                 foreach (var item in timeseriesDataRaw.NumericValues)
//                 {
//                     var parameterName = item.Key;
//                     var parameterValue = item.Value[i];
//                     var bufferItem = new TimeseriesBufferData
//                     {
//                         Timestamp = timestamp,
//                         ParameterName = parameterName,
//                         ParameterType = ParameterType.Numeric,
//                         ParameterValue = parameterValue.Value
//                     };
//                     buffer.Add(bufferItem);
//                 }
//
//                 foreach (var item in timeseriesDataRaw.StringValues)
//                 {
//                     var parameterName = item.Key;
//                     var parameterValue = item.Value[i];
//                     var bufferItem = new TimeseriesBufferData
//                     {
//                         Timestamp = timestamp,
//                         ParameterName = parameterName,
//                         ParameterType = ParameterType.String,
//                         ParameterValue = parameterValue
//                     };
//                     buffer.Add(bufferItem);
//                 }
//
//                 foreach (var item in timeseriesDataRaw.BinaryValues)
//                 {
//                     var parameterName = item.Key;
//                     var parameterValue = item.Value[i];
//                     var bufferItem = new TimeseriesBufferData
//                     {
//                         Timestamp = timestamp,
//                         ParameterName = parameterName,
//                         ParameterType = ParameterType.Binary,
//                         ParameterValue = parameterValue
//                     };
//                     buffer.Add(bufferItem);
//                 }
//
//                 foreach (var item in timeseriesDataRaw.TagValues)
//                 {
//                     var parameterName = item.Key;
//                     var parameterValue = item.Value[i];
//                     var bufferItem = new TimeseriesBufferData
//                     {
//                         Timestamp = timestamp,
//                         ParameterName = parameterName,
//                         ParameterType = ParameterType.Tag,
//                         ParameterValue = parameterValue
//                     };
//                     buffer.Add(bufferItem);
//                 }
//             }
//         }
//
//         public TimeseriesDataRaw ConvertToRawData()
//         {
//             // Group data by timestamp and parameter name
//             var groups = buffer.GroupBy(x => new { x.Timestamp, x.ParameterName });
//
//             // Initialize raw data arrays and dictionaries
//             var timestamps = groups.Select(x => x.Key.Timestamp).ToArray();
//             var numericValues = new Dictionary<string, double?[]>(groups.Select(x => x.Key.ParameterName).Distinct().ToDictionary(x => x, x => new double?[timestamps.Length]));
//             var stringValues = new Dictionary<string, string[]>(groups.Select(x => x.Key.ParameterName).Distinct().ToDictionary(x => x, x => new string[timestamps.Length]));
//             var binaryValues = new Dictionary<string, byte[][]>(groups.Select(x => x.Key.ParameterName).Distinct().ToDictionary(x => x, x => new byte[timestamps.Length][]));
//             var tagValues = new Dictionary<string, string[]>(groups.Select(x => x.Key.ParameterName).Distinct().ToDictionary(x => x, x => new string[timestamps.Length]));
//
//             // Populate raw data dictionaries with grouped data
//             foreach (var group in groups)
//             {
//                 var parameterName = group.Key.ParameterName;
//                 var index = Array.IndexOf(timestamps, group.Key.Timestamp);
//                 switch (group.First().ParameterType)
//                 {
//                     case ParameterType.Numeric:
//                         numericValues[parameterName][index] = (double?)group.First().ParameterValue;
//                         break;
//                     case ParameterType.String:
//                         stringValues[parameterName][index] = (string)group.First().ParameterValue;
//                         break;
//                     case ParameterType.Binary:
//                         binaryValues[parameterName][index] = (byte[])group.First().ParameterValue;
//                         break;
//                     case ParameterType.Tag:
//                         tagValues[parameterName][index] = (string)group.First().ParameterValue;
//                         break;
//                 }
//             }
//
//             return new TimeseriesDataRaw(
//                 0,
//                 timestamps,
//                 numericValues,
//                 stringValues,
//                 binaryValues,
//                 tagValues);
//         }
//
//         private struct TimeseriesBufferData
//         {
//             public long Timestamp { get; set; }
//             public string ParameterName { get; set; }
//             public ParameterType ParameterType { get; set; }
//             public object? ParameterValue { get; set; }
//         }
//     
//     
//         private enum ParameterType
//         {
//             Numeric,
//             String,
//             Tag,
//             Binary
//         }
//
//     }
//
// }