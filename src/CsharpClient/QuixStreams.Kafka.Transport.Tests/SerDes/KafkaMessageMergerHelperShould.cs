using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Quix.TestBase.Extensions;
using QuixStreams.Kafka.Transport.SerDes;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Kafka.Transport.Tests.SerDes
{
    public class KafkaMessageMergerHelperShould
    {
        private KafkaMessageSplitter splitter;
        private readonly ILogger<KafkaMessageMergerHelperShould> logger;

        public KafkaMessageMergerHelperShould(ITestOutputHelper output)
        {
            const int maxMsgLength = 50;
            this.splitter = new KafkaMessageSplitter(KafkaMessageSplitter.ExpectedHeaderSplitInfoSize + maxMsgLength);
            this.logger = output.ConvertToLogger<KafkaMessageMergerHelperShould>();
        }
        
        /// <summary>
        /// Always returns messages with ever increasing msg ids
        /// </summary>
        /// <param name="originalData"></param>
        /// <returns></returns>
        private IEnumerable<KafkaMessage> GetSplitData(out byte[] originalData)
        {
            var length = (int)Math.Ceiling(splitter.MaximumKafkaMessageSize * 5.5); // just a bit more than max;
            originalData = new byte[length];
            var random = new Random();
            random.NextBytes(originalData);

            var message = new KafkaMessage(null, originalData, null);
            return splitter.Split(message);
        }

        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_WithDataThatIsNotSplit_ShouldReturnSameBytes(PackageSerializationMode mode)
        {
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(), logger);
            var data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
            var message = new KafkaMessage(null, data, null);

            // Act
            var mergeResult = merger.TryMerge(message, out var _, out var result);

            // Assert
            mergeResult.Should().Be(MessageMergeResult.Unmerged);
            result.Value.Should().BeSameAs(data);
        }

        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_WithSplitDataNotLastMessage_ShouldReturnNull(PackageSerializationMode mode)
        {
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(), logger);
            var splitData = this.GetSplitData(out var data).ToList();

            // Act & Assert
            for (var index = 0; index < splitData.Count - 1; index++)
            {
                if (index == splitData.Count) break; // last message is tested elsewhere
                var segment = splitData[index];
                var mergeResult = merger.TryMerge(segment, out var _, out var result);
                mergeResult.Should().Be(MessageMergeResult.MergePending);
                result.Should().BeNull("because the merge is not complete yet");
            }
        }

        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_WithSplitDataThatIsLastMessageWithoutRest_ShouldReturnNull(PackageSerializationMode mode)
        {
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(), logger);
            var splitData = this.GetSplitData(out var data).ToList();

            // Act
            var mergeResult = merger.TryMerge(splitData[splitData.Count-1], out var _, out var result);

            // Arrange
            mergeResult.Should().Be(MessageMergeResult.Discarded);
            result.Should().BeNull("We do not have the other segments necessary to merge");
        }

        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_WithSplitDataThatIsLastMessageWithRest_ShouldReturnMerged(PackageSerializationMode mode)
        {
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(), logger);
            var splitData = this.GetSplitData(out var data).ToList();

            // Act
            for (var index = 0; index < splitData.Count - 1; index++)
            {
                var segment = splitData[index];
                merger.TryMerge(segment, out var _, out var _);
            }

            var mergeResult = merger.TryMerge(splitData[splitData.Count-1], out var _, out var result);

            // Arrange
            mergeResult.Should().Be(MessageMergeResult.Merged);
            result.Value.Should().BeEquivalentTo(data, "we have all necessary segments to merge");
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Purge_WithValidBufferId_ShouldPurgeAllSegments(PackageSerializationMode mode)
        {
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var buffer = new KafkaMessageBuffer();
            var merger = new KafkaMessageMergerHelper(buffer, this.logger);
            var splitData = this.GetSplitData(out var data).ToList();
            
            MergerBufferId bufferId = default;
            for (var index = 0; index < splitData.Count - 2; index++)
            {
                var segment = splitData[index];
                merger.TryMerge(segment, out var _, out var _);
            }

            merger.TryMerge(splitData[splitData.Count-2], out bufferId, out var result);
            

            buffer.Exists(bufferId).Should().BeTrue();

            // Act
            merger.Purge(bufferId);

            // Arrange
            buffer.Remove(bufferId, out var _, out var _).Should().BeNull();
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_InterlacingMessages_ShouldReturnMerged(PackageSerializationMode mode)
        {
            var uniqueMessageCount = 10;
            
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(uniqueMessageCount), logger);
            var splitData = Enumerable.Range(0, 10).Select(x=>
            new {
                Segments = this.GetSplitData(out var data).ToList(),
                Data = data
            }).ToList();

            var splitCount = splitData.First().Segments.Count;
            
            // Act
            for (var segmentIndex = 0; segmentIndex < splitCount - 1; segmentIndex++) // for each msg send one segment, then repeat till last -1
            {
                for (int msgIndex = 0; msgIndex < uniqueMessageCount; msgIndex++)
                {
                    var segment = splitData[msgIndex].Segments[segmentIndex];
                    merger.TryMerge(segment, out var _, out var _);
                }
            }
            // Arrange
            for (int msgIndex = 0; msgIndex < uniqueMessageCount; msgIndex++)
            {
                var sD = splitData[msgIndex];
                var mergeResult = merger.TryMerge(sD.Segments[sD.Segments.Count-1], out var _, out var result);
                mergeResult.Should().Be(MessageMergeResult.Merged);
                result.Value.Should().BeEquivalentTo(sD.Data, $"we have all necessary segments to merge msg {msgIndex}");
            }
        }
        
        [Theory]
        [InlineData(PackageSerializationMode.LegacyValue)]
        [InlineData(PackageSerializationMode.Header)]
        public void Merge_DifferentMessageGroupKey_ShouldReturnMerged(PackageSerializationMode mode)
        {
            var uniqueMessageCount = 10;
            
            // Arrange
            PackageSerializationSettings.Mode = mode;
            var merger = new KafkaMessageMergerHelper(new KafkaMessageBuffer(uniqueMessageCount), this.logger);
            var splitData1 = this.GetSplitData(out var data1).ToList();
            var splitData2 = this.GetSplitData(out var data2).ToList();

            // Act
            for (var index = 0; index < splitData1.Count - 1; index++)
            {
                var segment = splitData1[index];
                merger.TryMerge(segment, out var _, out var _);
            }
            for (var index = 0; index < splitData2.Count - 1; index++)
            {
                var segment = splitData2[index];
                merger.TryMerge(segment, out var _, out var _);
            }

            var mergeResult1 = merger.TryMerge(splitData1[splitData1.Count-1], out var _, out var result1);
            var mergeResult2 = merger.TryMerge(splitData2[splitData2.Count-1], out var _, out var result2);

            // Arrange
            mergeResult1.Should().Be(MessageMergeResult.Merged);
            result1.Value.Should().BeEquivalentTo(data1, "we have all necessary segments to merge");
            mergeResult2.Should().Be(MessageMergeResult.Merged);
            result2.Value.Should().BeEquivalentTo(data2, "we have all necessary segments to merge");
        }
    }
}