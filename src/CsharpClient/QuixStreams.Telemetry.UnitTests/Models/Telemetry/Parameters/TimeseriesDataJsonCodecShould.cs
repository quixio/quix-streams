using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using FluentAssertions;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Telemetry.Parameters.Codecs;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Telemetry.UnitTests.Models.Telemetry.Parameters
{
    public class TimeseriesDataJsonCodecShould
    {
        private readonly ITestOutputHelper output;

        public TimeseriesDataJsonCodecShould(ITestOutputHelper output)
        {
            this.output = output;
        }
        
        
        [Fact]
        public void Serialization_ThenDeserialization_ShouldResultInOriginalData()
        {
            var timeseriesData = new TimeseriesDataRaw()
            {
                Epoch = 150,
                Timestamps = Enumerable.Range(0, 1000).Select(x=> (long)x*11).ToDictionary(x=> new Random().Next(0, int.MaxValue), x=> x).OrderBy(x=> x.Key).Select(x=> x.Value).ToArray(),
                NumericValues = new Dictionary<string, double?[]>(),
                StringValues = new Dictionary<string, string[]>(),
                BinaryValues = new Dictionary<string, byte[][]>(),
                TagValues = new Dictionary<string, string[]>()
            };

            // Set a few duplicate timestamps
            timeseriesData.Timestamps[10] = timeseriesData.Timestamps[9];
            timeseriesData.Timestamps[11] = timeseriesData.Timestamps[9];
            timeseriesData.Timestamps[12] = timeseriesData.Timestamps[9];
            timeseriesData.Timestamps[13] = timeseriesData.Timestamps[9];

            var random = new Random();
            
            // Generate some tags
            var tagKeys = Enumerable.Range(0, 15).Select(x =>
            {
                var bytes = new byte[20];
                random.NextBytes(bytes);
                return Encoding.UTF8.GetString(bytes);
            }).ToArray();
            
            foreach (var tagKey in tagKeys)
            {
                var possibleTagValues = Enumerable.Range(0, 10).Select(x =>
                {
                    var bytes = new byte[20];
                    random.NextBytes(bytes);
                    return Encoding.UTF8.GetString(bytes);
                }).ToArray();
                var tagValue = new string[timeseriesData.Timestamps.Length];
                for (int i = 0; i < tagValue.Length; i++)
                {
                    tagValue[i] = random.Next(0, 2) == 1 ? null : possibleTagValues[random.Next(0, possibleTagValues.Length)];
                }

                timeseriesData.TagValues[tagKey] = tagValue;
            }


            // Generate some numeric values
            foreach (var i in Enumerable.Range(0, 25))
            {
                var name = $"Parameter_{i}";
                var values = new double?[timeseriesData.Timestamps.Length];
                for (int j = 0; j < values.Length; j++)
                {
                    values[j] = random.Next(0, j % 10 + 1) != 1 ? (double?)null : Math.Round(random.NextDouble()*10000, 5);
                }

                timeseriesData.NumericValues[name] = values;
            }
            
            // Generate some string values
            foreach (var i in Enumerable.Range(25, 25))
            {
                var availableValuesAsString = Enumerable.Range(0, 15).Select(x =>
                {
                    var bytes = new byte[20];
                    random.NextBytes(bytes);
                    return Encoding.UTF8.GetString(bytes);
                }).ToArray();
                var name = $"Parameter_{i}";
                var values = new string[timeseriesData.Timestamps.Length];
                for (int j = 0; j < values.Length; j++)
                {
                    values[j] = random.Next(0, j % 20 + 1) == 1 ? null : availableValuesAsString[random.Next(0, availableValuesAsString.Length)];
                }

                timeseriesData.StringValues[name] = values;
            }
            
            // Generate some binary values
            foreach (var i in Enumerable.Range(50, 5))
            {
                var availableValues = Enumerable.Range(0, 20).Select(x =>
                {
                    var bytes = new byte[20];
                    random.NextBytes(bytes);
                    return bytes;
                }).ToArray();
                var name = $"Parameter_{i}";
                for (int j = 0; j < availableValues.Length; j++)
                {
                    // null out every 5th or so
                    availableValues[j] = random.Next(0, j % 5) == 1 ? null : availableValues[j];
                }
                timeseriesData.BinaryValues[name] = availableValues;
            }

            var codec = new TimeseriesDataReadableCodec();
            var newCodecSw = Stopwatch.StartNew();
            var newCodecSerialized = codec.Serialize(timeseriesData);
            newCodecSw.Stop();
            var defaultCodecSw = Stopwatch.StartNew();
            var defaultCodec = new DefaultJsonCodec<TimeseriesDataRaw>();
            var defaultCodecSerialized = defaultCodec.Serialize(timeseriesData);
            defaultCodecSw.Stop();
            
            var deserialised = codec.Deserialize(newCodecSerialized);
            deserialised.Should().BeEquivalentTo(timeseriesData, o => o.WithoutStrictOrdering().Using<double?>(ctx =>
                {
                    if (ctx.Subject == null) ctx.Subject.Should().Be(ctx.Expectation);
                    else ctx.Subject.Should().BeApproximately(ctx.Expectation, 0.00001);;
                })
                .WhenTypeIs<double?>());
            
            var sizeDiff = (double) newCodecSerialized.Length / defaultCodecSerialized.Length;
            var timeDiff = (double)newCodecSw.ElapsedTicks / defaultCodecSw.ElapsedTicks;
            output.WriteLine($"Type specific codec is {sizeDiff:P} of generic in size.");
            output.WriteLine($"Type specific codec finished in {timeDiff:P} of the generic's time.");

            var compressedNew = Compress(newCodecSerialized);
            var compressedOld = Compress(defaultCodecSerialized);
            var sizeDiffComp = (double) compressedNew.Length / compressedOld.Length;
            var compressionRateNew = (double) compressedNew.Length / newCodecSerialized.Length;
            var compressionRateOld = (double) compressedOld.Length / defaultCodecSerialized.Length;            
        }
        
        static byte[] Compress(byte[] data)
        {
            using (var compressedStream = new MemoryStream())
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
            {
                zipStream.Write(data, 0, data.Length);
                zipStream.Close();
                return compressedStream.ToArray();
            }
        }

        static byte[] Decompress(byte[] data)
        {
            using (var compressedStream = new MemoryStream(data))
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var resultStream = new MemoryStream())
            {
                zipStream.CopyTo(resultStream);
                return resultStream.ToArray();
            }
        }
    }
}