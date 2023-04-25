using System;
using FluentAssertions;
using QuixStreams.State.Storage;
using QuixStreams.Streaming.States;
using Xunit;

namespace QuixStreams.Streaming.UnitTests.States
{
    public class TopicStateShould
    {
        [Fact]
        public void Constructor_ShouldCreateTopicState()
        {
            var topicState = new TopicState<int>(new InMemoryStateStorage(), key => -1);

            topicState.Should().NotBeNull();

            topicState["TestNotExistingKey"].Should().Be(-1);
            topicState["AnotherKey"] += 20;
            topicState["AnotherKey"].Should().Be(19);
        }

        [Fact]
        public void Constructor_WithNullStateStorage_ShouldThrowArgumentNullException()
        {
            Action nullTopicName = () => new TopicState<int>(null, key => -1);

            nullTopicName.Should().Throw<ArgumentNullException>().WithMessage("*storage*");
        }
        
        [Fact]
        public void ComplexModifications_ShouldHaveExpectedValues()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var topicState = new TopicState<string>(storage, null);
            
            // Act 1
            topicState.Add("Key1", "Value1"); // will keep as is, before clear
            topicState.Add("Key2", "Value2"); // will remove, before clear
            topicState.Add("Key3", "Value3"); // will override, before clear
            topicState["Key4"] = "Value4"; // leave as is
            topicState.Remove("Key2");
            topicState["Key3"] = "Value3b";
            topicState.Clear();
            topicState.Add("Key5", "Value5"); // will keep as is
            topicState.Add("Key6", "Value6"); // will remove
            topicState.Add("Key7", "Value7"); // will override
            topicState["Key8"] = "Value8"; // leave as is
            topicState.Remove("Key5");
            topicState["Key6"] = "Value6b";
            
            // Assert 1
            topicState.Count.Should().Be(3);
            topicState["Key6"].Should().Be("Value6b");
            topicState["Key7"].Should().Be("Value7");
            topicState["Key8"].Should().Be("Value8");
            
            // Act 2
            topicState.Flush();
            topicState["Key8"] = "Value8";
            topicState.Flush();
            
            // Assert 2
            topicState.Count.Should().Be(3);
            topicState["Key6"].Should().Be("Value6b");
            topicState["Key7"].Should().Be("Value7");
            topicState["Key8"].Should().Be("Value8");

            // Act 3
            var topicState2 = new TopicState<string>(storage, null);

            // Assert 3
            topicState2.Count.Should().Be(3);
            topicState2["Key6"].Should().Be("Value6b");
            topicState2["Key7"].Should().Be("Value7");
            topicState2["Key8"].Should().Be("Value8");
        }
    }
}