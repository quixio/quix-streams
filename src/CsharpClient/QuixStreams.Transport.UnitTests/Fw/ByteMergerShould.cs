using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using QuixStreams.Transport.Fw;
using Xunit;

namespace QuixStreams.Transport.UnitTests.Fw
{
    public class ByteMergerShould
    {
        private ByteSplitter splitter;

        public ByteMergerShould()
        {
            const int maxMsgLength = 50;
            this.splitter = new ByteSplitter(maxMsgLength);
        }
        
        /// <summary>
        /// Always returns messages with ever increasing msg ids
        /// </summary>
        /// <param name="originalData"></param>
        /// <returns></returns>
        private IEnumerable<byte[]> GetSplitData(out byte[] originalData)
        {
            var length = splitter.AbsoluteMaxMessageSize - 10; // just a bit less than max;
            originalData = new byte[length];
            var random = new Random();
            random.NextBytes(originalData);

            return splitter.Split(originalData);
        }
        
        /// <summary>
        /// Always returns message with msg id 1
        /// </summary>
        /// <param name="originalData"></param>
        /// <returns></returns>
        private IEnumerable<byte[]> GetNewSplitData(out byte[] originalData)
        {
            var length = splitter.AbsoluteMaxMessageSize - 10; // just a bit less than max;
            originalData = new byte[length];
            var random = new Random();
            random.NextBytes(originalData);

            return splitter.Split(originalData);
        }

        [Fact]
        public void Merge_WithDataThatIsNotSplit_ShouldReturnSameBytes()
        {
            // Arrange
            var merger = new ByteMerger(new MergeBuffer());
            var data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};

            // Act
            var result = merger.Merge(data, string.Empty, out var _);

            // Assert
            result.Should().BeSameAs(data);
        }

        [Fact]
        public void Merge_WithSplitDataNotLastMessage_ShouldReturnNull()
        {
            // Arrange
            var merger = new ByteMerger(new MergeBuffer());
            var splitData = this.GetSplitData(out var data).ToList();

            // Act & Assert
            for (var index = 0; index < splitData.Count - 1; index++)
            {
                if (index == splitData.Count) break; // last message is tested elsewhere
                var segment = splitData[index];
                var result = merger.Merge(segment, string.Empty, out var _);
                result.Should().BeNull("because the merge is not complete yet");
            }
        }

        [Fact]
        public void Merge_WithSplitDataThatIsLastMessageWithoutRest_ShouldReturnNull()
        {
            // Arrange
            var merger = new ByteMerger(new MergeBuffer());
            var splitData = this.GetSplitData(out var data).ToList();

            // Act
            var result = merger.Merge(splitData[^1], string.Empty, out var _);

            // Arrange
            result.Should().BeNull("We do not have the other segments necessary to merge");
        }

        [Fact]
        public void Merge_WithSplitDataThatIsLastMessageWithRest_ShouldReturnMerged()
        {
            // Arrange
            var merger = new ByteMerger(new MergeBuffer());
            var splitData = this.GetSplitData(out var data).ToList();

            // Act
            for (var index = 0; index < splitData.Count - 1; index++)
            {
                var segment = splitData[index];
                merger.Merge(segment, string.Empty, out var _);
            }

            var result = merger.Merge(splitData[^1], string.Empty, out var _);

            // Arrange
            result.Should().BeEquivalentTo(data, "we have all necessary segments to merge");
        }
        
        [Fact]
        public void Purge_WithValidBufferId_ShouldPurgeAllSegments()
        {
            // Arrange
            var buffer = new MergeBuffer();
            var merger = new ByteMerger(buffer);
            var splitData = this.GetSplitData(out var data).ToList();
            ByteSplitter.TryGetSplitDetails(splitData[0], out var msgId, out var _, out var _, out var _);
            var msgGroupKey = "SomeRandomKey";
            
            for (var index = 0; index < splitData.Count - 2; index++)
            {
                var segment = splitData[index];
                merger.Merge(segment, msgGroupKey, out var _);
            }

            merger.Merge(splitData[^2], msgGroupKey, out var bufferId);

            buffer.Exists(msgGroupKey, msgId).Should().BeTrue();

            // Act
            merger.Purge(bufferId);

            // Arrange
            buffer.Remove(msgGroupKey, msgId, out var _, out var _).Should().BeNull();
        }
        
        [Fact]
        public void Merge_InterlacingMessages_ShouldReturnMerged()
        {
            var uniqueMessageCount = 10;
            
            // Arrange
            var merger = new ByteMerger(new MergeBuffer(uniqueMessageCount));
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
                    merger.Merge(segment, string.Empty, out var _);
                }
            }
            // Arrange
            for (int msgIndex = 0; msgIndex < uniqueMessageCount; msgIndex++)
            {
                var sD = splitData[msgIndex];
                var result = merger.Merge(sD.Segments[^1], string.Empty, out var _);
                result.Should().BeEquivalentTo(sD.Data, $"we have all necessary segments to merge msg {msgIndex}");
            }
        }
        
        [Fact]
        public void Merge_DifferentMessageGroupKey_ShouldReturnMerged()
        {
            var uniqueMessageCount = 10;
            
            // Arrange
            var merger = new ByteMerger(new MergeBuffer(uniqueMessageCount));
            var splitData1 = this.GetNewSplitData(out var data1).ToList();
            var splitData2 = this.GetNewSplitData(out var data2).ToList();

            // Act
            for (var index = 0; index < splitData1.Count - 1; index++)
            {
                var segment = splitData1[index];
                merger.Merge(segment, "Group 1", out var _);
            }
            for (var index = 0; index < splitData2.Count - 1; index++)
            {
                var segment = splitData2[index];
                merger.Merge(segment, "Group 2", out var _);
            }

            var result1 = merger.Merge(splitData1[^1], "Group 1", out var _);
            var result2 = merger.Merge(splitData2[^1], "Group 2", out var _);

            // Arrange
            result1.Should().BeEquivalentTo(data1, "we have all necessary segments to merge");
            result2.Should().BeEquivalentTo(data2, "we have all necessary segments to merge");
        }
    }
}