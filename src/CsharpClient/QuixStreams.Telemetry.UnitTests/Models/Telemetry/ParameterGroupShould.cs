using System;
using FluentAssertions;
using QuixStreams.Telemetry.Models;
using Xunit;

namespace QuixStreams.Telemetry.UnitTests.Models.Telemetry
{
    public class ParameterGroupShould
    {
        [Theory]
        [InlineData("IamIn/Valid")]
        [InlineData("IamIn\\Validtoo")]
        public void Name_WithInvalidCharacter_ShouldNotThrowException(string name)
        {
            var tGroup = new ParameterGroupDefinition();
            // Act
            Action action = () => tGroup.Name = name;
            
            // Assert
            action.Should().Throw<ArgumentOutOfRangeException>();
        }
    }
}