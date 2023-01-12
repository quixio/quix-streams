using System.Collections.Generic;
using FluentAssertions;
using Quix.Sdk.Streaming.Models;
using Xunit;

namespace Quix.Sdk.Streaming.UnitTests.Models
{
    public class ParameterDataTimestampShould
    {
        
        [Fact]
        public void AddTags_NullAndEmptyTags_ShouldNotThrowException()
        {

            // Act
            var pd = new ParameterData().AddTimestampNanoseconds(100);
            pd.AddTag("test4", "val4");
            pd.AddTag("test2", "val5");
            pd.AddTags(null);
            pd.AddTags(new Dictionary<string, string>());

            // Assert
            pd.Tags.Count.Should().Be(2);
            pd.Tags["test2"].Should().Be("val5");
            pd.Tags["test4"].Should().Be("val4");

        }
        
        [Fact]
        public void AddTags_WithTags_ShouldNotThrowException()
        {
            // Arrange
            var data = new ParameterData().AddTimestampNanoseconds(100);
            data.AddTag("test1", "val1")
            .AddTag("test2", "val2")
            .AddTag("test3", "val3");

            // Act
            var pd = new ParameterData().AddTimestampNanoseconds(100);
            pd.AddTag("test4", "val4");
            pd.AddTag("test2", "val5");
            pd.AddTags(data.Tags);

            // Assert
            pd.Tags.Count.Should().Be(4);
            pd.Tags["test1"].Should().Be("val1");
            pd.Tags["test2"].Should().Be("val2");
            pd.Tags["test3"].Should().Be("val3");
            pd.Tags["test4"].Should().Be("val4");

        }
    }
}