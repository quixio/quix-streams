using FluentAssertions;
using QuixStreams.Streaming.Utils;
using Xunit;

namespace QuixStreams.Streaming.IntegrationTests
{
    public class QuixUtilsShould
    {
        [Theory]
        [InlineData("myorganisation-andworkspace-some-topic-name", "myorganisation-andworkspace-")] // normal orgid-wsid-topicname combo
        [InlineData("myorganisation-andworkspace", null, false)] // not enough segments
        [InlineData("myorganisation-0-workspace1-weather-topic", "myorganisation-0-workspace1-")] // orgid-hashoforgid-wsid-topicname combo with minimum hash length
        [InlineData("myorganisation-0123457-workspace1-weather-topic", "myorganisation-0123457-workspace1-")] // orgid-hashoforgid-wsid-topicname combo with maximum hash length
        [InlineData("myorganisation-workspace1-0-weather-topic", "myorganisation-workspace1-0-")] // orgid-wsid-hashofwsid-topicname combo with minimum hash length
        [InlineData("myorganisation-workspace1-01234567-weather-topic", "myorganisation-workspace1-01234567-")] // orgid-wsid-hashofwsid-topicname combo with maximum hash length
        [InlineData("myorganisation-0-workspace1-0-weather-topic", "myorganisation-0-workspace1-0-")] // orgid-hashoforgid-wsid-hashofwsid-topicname combo with minimum hash length
        [InlineData("myorganisation-01234567-workspace1-01234567-weather-topic", "myorganisation-01234567-workspace1-01234567-")] // orgid-hashoforgid-wsid-hashofwsid-topicname combo with maximum hash length
        // some known limitations of the helper tested as 'working' the wrong way
        [InlineData("myorganisation-01234567-workspace1-01234567", "myorganisation-01234567-workspace1-")] // orgid-hashoforgid-wsid-hashofwsid where topicname is missing
        [InlineData("myorganisation-01234567-workspace1", "myorganisation-01234567-")] // orgid-hashoforgid-wsid where topicname is missing
        public void TryGetWorkspaceId_WithGivenTopicId_ShouldReturnExpectedWorkspaceId(string topicId, string expectedWorkspaceId, bool canbeConverted = true)
        {
            var success = QuixUtils.TryGetWorkspaceIdPrefix(topicId, out var actualWorkspaceId);
            success.Should().Be(canbeConverted);
            actualWorkspaceId.Should().Be(expectedWorkspaceId);
        }
    }
}