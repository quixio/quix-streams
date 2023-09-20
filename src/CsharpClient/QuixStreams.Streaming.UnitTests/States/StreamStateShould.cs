using System;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class StreamStateShould
    {
        [Fact]
        public void Constructor_ShouldCreateStreamState()
        {
            var streamState = new StreamDictionaryState<int>(InMemoryStorage.GetStateStorage("myStream", "stateName"), missingStateKey => -1, NullLoggerFactory.Instance);

            streamState.Should().NotBeNull();

            streamState["TestNotExistingKey"].Should().Be(-1);
            streamState["AnotherKey"] += 20;
            streamState["AnotherKey"].Should().Be(19);
        }

        [Fact]
        public void Constructor_WithNullStateStorage_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new StreamDictionaryState<int>(null, missingStateKey => -1, NullLoggerFactory.Instance);

            nullTopicName.Should().Throw<ArgumentNullException>().WithMessage("*storage*");
        }

        [Fact]
        public void ComplexModifications_ShouldHaveExpectedValues()
        {
            // Arrange
            var storage = InMemoryStorage.GetStateStorage("myStream", "myState");
            var streamState = new StreamDictionaryState<string>(storage, null, NullLoggerFactory.Instance);

            // Act 1
            streamState.Add("Key1", "Value1"); // will keep as is, before clear
            streamState.Add("Key2", "Value2"); // will remove, before clear
            streamState.Add("Key3", "Value3"); // will override, before clear
            streamState["Key4"] = "Value4"; // leave as is
            streamState.Remove("Key2");
            streamState["Key3"] = "Value3b";
            streamState.Clear();
            streamState.Add("Key5", "Value5"); // will keep as is
            streamState.Add("Key6", "Value6"); // will remove
            streamState.Add("Key7", "Value7"); // will override
            streamState["Key8"] = "Value8"; // leave as is
            streamState.Remove("Key5");
            streamState["Key6"] = "Value6b";

            // Assert 1
            streamState.Count.Should().Be(3);
            streamState["Key6"].Should().Be("Value6b");
            streamState["Key7"].Should().Be("Value7");
            streamState["Key8"].Should().Be("Value8");

            // Act 2
            streamState.Flush();
            streamState["Key8"] = "Value8";
            streamState.Flush();

            // Assert 2
            streamState.Count.Should().Be(3);
            streamState["Key6"].Should().Be("Value6b");
            streamState["Key7"].Should().Be("Value7");
            streamState["Key8"].Should().Be("Value8");

            // Act 3
            var streamState2 = new StreamDictionaryState<string>(storage, null, NullLoggerFactory.Instance);

            // Assert 3
            streamState2.Count.Should().Be(3);
            streamState2["Key6"].Should().Be("Value6b");
            streamState2["Key7"].Should().Be("Value7");
            streamState2["Key8"].Should().Be("Value8");
        }
    }
}