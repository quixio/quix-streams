using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace QuixStreams.Streaming.UnitTests;

public class StreamingClientShould
{
    [Theory]
    [InlineData("confluent-testTopic")]
    [InlineData("quixdev-secondTest")]
    public void GetTopicConsumer_ShouldUseClientId(string topicName)
    {
        // Arrange
        var messageHandler = new MockHttpMessageHandler(new Dictionary<string, string>()
        {
            { "/workspaces", workspaces },
            { "/topics", topics }
        });
        var client = new HttpClient(messageHandler);
        var streamingClient = new QuixStreamingClient(httpClient: client, token: "faketoken");

        // Act
        var topicConsumer = streamingClient.GetTopicConsumer(topicName);

        // Assert
        topicConsumer.Should().NotBeNull();
    }

    private string topics = @"
[
    {
        ""id"": ""confluent-testTopic"",
        ""name"": ""confluent-testTopic"",
        ""workspaceId"": ""confluent"",
        ""status"": ""Ready"",
    },
    {
        ""id"": ""quixdev-secondTest"",
        ""name"": ""quixdev-secondTest"",
        ""workspaceId"": ""quixdev"",
        ""status"": ""Ready"",
    }
]";

    private string workspaces = @"
[
    {
        ""workspaceId"": ""confluent"",
        ""name"": ""ConfluentCloud example"",
        ""status"": ""Ready"",
        ""brokerType"": ""ConfluentCloud"",
        ""broker"": {
            ""address"": ""xxxxx:9092"",
            ""securityMode"": ""SaslSsl"",
            ""sslPassword"": """",
            ""saslMechanism"": ""Plain"",
            ""username"": ""xxxxx"",
            ""password"": ""xxxxx"",
            ""hasCertificate"": false
        },
        ""workspaceClassId"": ""Standard"",
        ""storageClassId"": ""Standard"",
        ""createdAt"": ""2023-07-04T15:20:04.58Z"",
        ""brokerSettings"": {
            ""brokerType"": ""ConfluentCloud"",
            ""syncTopics"": false,
            ""confluentCloudSettings"": {
                ""apiKey"": ""xxxxx"",
                ""apiSecret"": ""xxxxx"",
                ""clusterID"": ""xxxxx"",
                ""bootstrapServer"": ""xxxxx:9092"",
                ""restEndpoint"": ""xxxxx"",
                ""clientID"": ""testclientid""
            }
        },
        ""repositoryId"": ""xxxxx"",
        ""branch"": ""main"",
        ""environmentName"": ""prod"",
        ""version"": 2,
        ""branchProtected"": true
    },
    {
        ""workspaceId"": ""quixdev"",
        ""name"": ""Shared Kafka Workspace"",
        ""status"": ""Ready"",
        ""brokerType"": ""SharedKafka"",
        ""broker"": {
            ""address"": ""xxxx:9092"",
            ""securityMode"": ""SaslSsl"",
            ""sslPassword"": ""xxxx"",
            ""saslMechanism"": ""ScramSha256"",
            ""username"": ""xxxx"",
            ""password"": ""xxxx"",
            ""hasCertificate"": false
        },
        ""workspaceClassId"": ""Standard"",
        ""storageClassId"": ""Standard"",
        ""createdAt"": ""2023-07-07T14:02:00.932Z"",
        ""brokerSettings"": {
            ""brokerType"": ""SharedKafka"",
            ""syncTopics"": false
        },
        ""repositoryId"": ""xxxxxxxxxx"",
        ""branch"": ""chris"",
        ""environmentName"": ""Testing"",
        ""version"": 2,
        ""branchProtected"": false
    }
]";

    private class MockHttpMessageHandler : HttpMessageHandler
    {
        private readonly Dictionary<string, string> responses;

        public MockHttpMessageHandler(Dictionary<string, string> responses)
        {
            this.responses = responses;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            foreach (var keyValuePair in responses)
            {
                if (request.RequestUri != null && request.RequestUri.ToString().Contains(keyValuePair.Key))
                {
                    return new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.OK,
                        Content = new StringContent(keyValuePair.Value)
                    };
                }
            }

            throw new Exception("URL not found");
        }
    }
}