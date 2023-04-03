using System;
using System.Threading.Tasks;
using FluentAssertions;
using QuixStreams.State.Storage;
using Xunit;

namespace QuixStreams.State.UnitTests
{
    public class StateShould
    {
        [Fact]
        public void Constructor_UsingNonEmptyState_ShouldLoadState()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            storage.SetAsync("existing", new StateValue("whatever"));
            var state = new State(storage);

            // Assert
            state.Count.Should().Be(1);
            state["existing"].StringValue.Should().BeEquivalentTo("whatever");
        }
        
        [Fact]
        public void Add_StateValue_ShouldIncreaseCount()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);

            // Act
            state.Add("key", new StateValue("value"));

            // Assert
            state.Count.Should().Be(1);
        }

        [Fact]
        public void Remove_StateValue_ShouldDecreaseCount()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key", new StateValue("value"));

            // Act
            state.Remove("key");

            // Assert
            state.Count.Should().Be(0);
        }

        [Fact]
        public void Clear_State_ShouldRemoveAllItems()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key1", new StateValue("value1"));
            state.Add("key2", new StateValue("value2"));

            // Act
            state.Clear();

            // Assert
            state.Count.Should().Be(0);
        }

        [Fact]
        public async Task Flush_State_ShouldPersistChangesToStorage()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key1", new StateValue("value1"));
            state.Add("key2", new StateValue("value2"));

            // Act
            state.Flush();

            // Assert
            (await storage.Count()).Should().Be(2);
        }
        
        [Fact]
        public async Task Flush_ClearBeforeFlush_ShouldClearStorage()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key1", new StateValue("value1"));
            state.Add("key2", new StateValue("value2"));
            state.Flush();
            state.Clear();

            // Act
            state.Flush();

            // Assert
            (await storage.Count()).Should().Be(0);
        }

        [Fact]
        public void State_WithNullStorage_ShouldThrowArgumentNullException()
        {
            // Act
            Action act = () => new State(null);

            // Assert
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void ContainsKey_KeyExists_ShouldReturnTrue()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key", new StateValue("value"));

            // Act
            bool containsKey = state.ContainsKey("key");

            // Assert
            containsKey.Should().BeTrue();
        }

        [Fact]
        public void ContainsKey_KeyDoesNotExist_ShouldReturnFalse()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);

            // Act
            bool containsKey = state.ContainsKey("key");

            // Assert
            containsKey.Should().BeFalse();
        }

        [Fact]
        public void TryGetValue_KeyExists_ShouldReturnTrueAndValue()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key", new StateValue("value"));

            // Act
            bool success = state.TryGetValue("key", out StateValue value);

            // Assert
            success.Should().BeTrue();
            value.StringValue.Should().BeEquivalentTo("value");
        }

        [Fact]
        public void TryGetValue_KeyDoesNotExist_ShouldReturnFalseAndDefaultValue()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);

            // Act
            bool success = state.TryGetValue("key", out StateValue value);

            // Assert
            success.Should().BeFalse();
            value.Should().BeNull();
        }

        [Fact]
        public void Indexer_GetAndSet_ShouldWorkCorrectly()
        {
            // Arrange
            var storage = new InMemoryStateStorage();
            var state = new State(storage);
            state.Add("key", new StateValue("value"));

            // Act
            state["key"] = new StateValue("updatedValue");
            StateValue value = state["key"];

            // Assert
            value.StringValue.Should().BeEquivalentTo("updatedValue");
        }
    }
}