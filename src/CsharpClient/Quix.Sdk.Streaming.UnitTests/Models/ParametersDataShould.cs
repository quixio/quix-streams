﻿using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;
using Quix.Sdk.Streaming.Models;
using FluentAssertions.Equivalency;
using System.Linq;
using System.Text;

namespace Quix.Sdk.Streaming.UnitTests.Models
{
    public class ParametersDataShould
    {
        [Fact]
        public void Clone_ParameterData_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var data = new ParameterData();
            data.AddTimestampMilliseconds(100)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            // Act
            var cloned = data.Clone();

            // Assert
            data.Should().BeEquivalentTo(cloned);
        }

        [Fact]
        public void Clone_WithParameterFilter_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var data = new ParameterData();
            data.AddTimestampMilliseconds(100)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            data.AddTimestampMilliseconds(200)
                .AddValue("param2", 20)
                .AddValue("param4", "40")
                .AddTag("tag1", "value1");

            // Act
            var filtered = data.Clone("param1", "param3");

            // Assert
            filtered.Timestamps.Count.Should().Be(1);

            data.rawData.NumericValues.Remove("param2");
            data.rawData.StringValues.Remove("param4");
            data.Timestamps.RemoveAt(1);

            data.Timestamps.Count.Should().Be(filtered.Timestamps.Count);
            data.Should().BeEquivalentTo(filtered);
        }

        [Fact]
        public void Copy_WithParameterFilter_OnProcessData_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var data = new ParameterData();
            data.AddTimestamp(DateTime.UtcNow)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            data.AddTimestampMilliseconds(200)
                .AddValue("param2", 20)
                .AddValue("param4", "40")
                .AddTag("tag1", "value1");

            // Act
            var processData = data.ConvertToProcessData();
            var filtered = new ParameterData(processData, new[] { "param1", "param3" });

            // Assert
            filtered.Timestamps.Count.Should().Be(1);

            data.rawData.NumericValues.Remove("param2");
            data.rawData.StringValues.Remove("param4");
            data.Timestamps.RemoveAt(1);

            data.Should().BeEquivalentTo(filtered, options => options.Including(info => info.WhichGetterHas(FluentAssertions.Common.CSharpAccessModifier.Public)));
        }

        [Fact]
        public void ConvertToProcessData_WithDuplicatedTimestamps_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var dataDuplicatedTimestamps = new ParameterData();
            dataDuplicatedTimestamps.AddTimestampMilliseconds(100)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param3", "3")
                .AddValue("param4", "4")
                .AddTag("tag1", "value1");

            dataDuplicatedTimestamps.AddTimestampMilliseconds(100)
                .AddValue("param1", 2)
                .AddValue("param2", 3)
                .AddValue("param3", "4")
                .AddValue("param4", "5")
                .AddValue("param5", new byte[] {1,2,3})
                .AddTag("tag1", "value1");

            dataDuplicatedTimestamps.AddTimestampMilliseconds(100)
                .AddValue("param1", 3)
                .AddValue("param2", 4)
                .AddValue("param3", "5")
                .AddValue("param4", "6")
                .AddTag("tag1", "value1");

            dataDuplicatedTimestamps.AddTimestampMilliseconds(200)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param4", "4");

            dataDuplicatedTimestamps.AddTimestampMilliseconds(200)
                .AddValue("param1", 2)
                .AddValue("param2", 3)
                .AddValue("param3", "4")
                .AddTag("tag1", "value2");


            var dataWithoutDuplicatedTimestamps = new ParameterData();
            dataWithoutDuplicatedTimestamps.AddTimestampMilliseconds(100)
                .AddValue("param1", 3)
                .AddValue("param2", 4)
                .AddValue("param3", "5")
                .AddValue("param4", "6")
                .AddValue("param5", new byte[] {1,2,3})
                .AddTag("tag1", "value1");


            dataWithoutDuplicatedTimestamps.AddTimestampMilliseconds(200)
                .AddValue("param1", 1)
                .AddValue("param2", 2)
                .AddValue("param4", "4");

            dataWithoutDuplicatedTimestamps.AddTimestampMilliseconds(200)
                .AddValue("param1", 2)
                .AddValue("param2", 3)
                .AddValue("param3", "4")
                .AddTag("tag1", "value2");

            // Act
            var processData1 = dataDuplicatedTimestamps.ConvertToProcessData();
            var processData2 = dataWithoutDuplicatedTimestamps.ConvertToProcessData();

            // Assert
            processData1.Timestamps.Length.Should().Be(3);
            processData1.BinaryValues.Should().NotBeEmpty();
            processData1.StringValues.Should().NotBeEmpty();
            processData1.NumericValues.Should().NotBeEmpty();
            processData1.Should().BeEquivalentTo(processData2);
        }

        [Fact]
        public void LoadFromProcessData_WithDuplicatedTimestamps_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var dataDuplicatedTimestamps = new Sdk.Process.Models.ParameterDataRaw()
            {
                Epoch = 0,
                Timestamps = new long[] { 100, 100, 100, 200, 200 },
                NumericValues = new Dictionary<string, double?[]>()
                {
                    { "param1", new double?[] { 1, 2, 3, 1, 2 } },
                    { "param2", new double?[] { 2, 3, 4, 2, 3 } },
                },
                StringValues = new Dictionary<string, string[]>()
                {
                    { "param3", new string[] { "3", "4", "5", null, "4" } },
                    { "param4", new string[] { "4", "5", "6", "4", null } },
                },
                TagValues = new Dictionary<string, string[]> ()
                {
                    { "tag1", new string[] { "value1", "value1", "value1", null, "value2" } }
                }
            };

            var dataWithoutDuplicatedTimestamps = new Sdk.Process.Models.ParameterDataRaw()
            {
                Epoch = 0,
                Timestamps = new long[] { 100 , 200, 200 },
                NumericValues = new Dictionary<string, double?[]>()
                {
                    { "param1", new double?[] { 3, 1, 2 } },
                    { "param2", new double?[] { 4, 2, 3 } },
                },
                StringValues = new Dictionary<string, string[]>()
                {
                    { "param3", new string[] { "5", null, "4" } },
                    { "param4", new string[] { "6", "4", null } },
                },
                TagValues = new Dictionary<string, string[]>()
                {
                    { "tag1", new string[] { "value1", null, "value2" } }
                }
            };


            // Act
            var data1 = new ParameterData(dataDuplicatedTimestamps);
            var data2 = new ParameterData(dataWithoutDuplicatedTimestamps);

            // Assert
            data1.Should().BeEquivalentTo(data2, options => options.Including(info => info.WhichGetterHas(FluentAssertions.Common.CSharpAccessModifier.Public)));
        }

        [Fact]
        public void LoadFromProcessData_WithNullValues_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var dataWithNulls = new Sdk.Process.Models.ParameterDataRaw()
            {
                Epoch = 0,
                Timestamps = new long[] { 100, 200, 300, 400, 500 },
                NumericValues = new Dictionary<string, double?[]>()
                {
                    { "param1", new double?[] { 1, null, 3, null, 5 } },
                    { "param2", new double?[] { null, 2, null, 4, null } },
                },
                StringValues = new Dictionary<string, string[]>()
                {
                    { "param3", new string[] { "1", null, "3", null, "5" } },
                    { "param4", new string[] { null, "2", null, "4", null } },
                },
                TagValues = new Dictionary<string, string[]>()
                {
                    { "tag1", new string[] { "value1", null, "value3", null, "value5" } }
                }
            };

            var expectedData = new ParameterData();
            expectedData.AddTimestampNanoseconds(100)
                .AddValue("param1", 1)
                .AddValue("param3", "1")
                .AddTag("tag1", "value1");

            expectedData.AddTimestampNanoseconds(200)
                .AddValue("param2", 2)
                .AddValue("param4", "2");

            expectedData.AddTimestampNanoseconds(300)
                .AddValue("param1", 3)
                .AddValue("param3", "3")
                .AddTag("tag1", "value3");

            expectedData.AddTimestampNanoseconds(400)
                .AddValue("param2", 4)
                .AddValue("param4", "4");

            expectedData.AddTimestampNanoseconds(500)
                .AddValue("param1", 5)
                .AddValue("param3", "5")
                .AddTag("tag1", "value5");

            // Act
            var data1 = new ParameterData(dataWithNulls);

            // Assert
            data1.ConvertToProcessData().Should().BeEquivalentTo(dataWithNulls);
            data1.ConvertToProcessData().Should().BeEquivalentTo(expectedData.ConvertToProcessData());
        }


        [Fact]
        public void ConvertToProcessData_WithDuplicatedTimestampsAndDifferentTags_ShouldCreateInstanceAsExpected()
        {
            // Arrange
            var dataDuplicatedTimestamps = new ParameterData();

            dataDuplicatedTimestamps.AddTimestampMilliseconds(100).AddValue("a", "1").AddTag("c", "val1");
            dataDuplicatedTimestamps.AddTimestampMilliseconds(100).AddValue("b", "1").AddTag("c", "val1");
            dataDuplicatedTimestamps.AddTimestampMilliseconds(100).AddValue("b", "3").AddTag("c", "val2");
            dataDuplicatedTimestamps.AddTimestampMilliseconds(100).AddValue("b", "2").AddTag("c", "val1");

            var expected = new ParameterData();
            expected.AddTimestampMilliseconds(100).AddValue("a", "1").AddValue("b", "2").AddTag("c", "val1");
            expected.AddTimestampMilliseconds(100).AddValue("b", "3").AddTag("c", "val2");

            // Act
            var processData1 = dataDuplicatedTimestamps.ConvertToProcessData();
            var processData2 = expected.ConvertToProcessData();

            // Assert
            processData1.Should().BeEquivalentTo(processData2);
        }

        [Fact]
        public void ParameterData20_EqualityComparison_ShouldReturnTrue()
        {
            // Arrange
            var data = GenerateParameterData(0, 20);

            // Act
            var dataCloned = data.Clone();

            // Assert
            dataCloned.Should().BeEquivalentTo(data);
        }

        [Fact]
        public void ParameterDataRaw20_ParameterDataRaw100_EqualityComparison_ShouldReturnTrue()
        {
            // Arrange
            var data100 = GenerateParameterData(0, 20);
            var data20 = GenerateParameterData(0, 20, 20);

            // Act

            // Assert
            data100.Should().BeEquivalentTo(data20);
        }

        [Fact]
        public void ParameterDataWithEpoch_ParameterDataWithoutEpoch_EqualityComparison_ShouldReturnTrue()
        {
            // Arrange
            var dataEpoch = GenerateParameterData(0, 20, 0, 1000, true);
            var dataNoEpoch = GenerateParameterData(0, 20, 0, 1000, false);

            // Act

            // Assert
            dataEpoch.Should().BeEquivalentTo(dataNoEpoch);
        }

        private static Sdk.Streaming.Models.ParameterData GenerateParameterData(int offset, int amount, int capacity = 0, long epoch = 0, bool includeEpoch = false)
        {
            var pData = new ParameterData();
            if (capacity > 0)
            {
                pData = new ParameterData(capacity);
            }

            if (epoch > 0 && !includeEpoch)
            {
                pData.rawData.Epoch = epoch;
            }

            var tags = Enumerable.Range(0, 2).Select(k => "tag" + k);

            for (int i = 0; i < amount; i++)
            {
                long time = i;

                if (includeEpoch)
                {
                    time += epoch;
                }

                var ts = pData.AddTimestampNanoseconds(time);
                foreach (var parameter in Enumerable.Range(0, 2).Select(k => "p" + k))
                {
                    ts.AddValue(parameter, (offset + i) * 10.0);
                }
                foreach (var parameter in Enumerable.Range(2, 2).Select(k => "p" + k))
                {
                    ts.AddValue(parameter, ((offset + i) * 10.0).ToString());
                }
                foreach (var parameter in Enumerable.Range(4, 2).Select(k => "p" + k))
                {
                    ts.AddValue(parameter, Encoding.UTF8.GetBytes(((offset + i) * 10.0).ToString()));
                }
                foreach (var tag in tags)
                {
                    ts.AddTag(tag, "tagValue" + i);
                }
            }

            return pData;
        }

    }
}
