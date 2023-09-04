using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace QuixStreams.Streaming.UnitTests
{

    public class StreamingClientShould
    {
        [Theory]
        [InlineData("confluent-testTopic")]
        [InlineData("quixdev-secondTest")]
        [InlineData("different-topic")]
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
    },
    {
        ""id"": ""different-topic"",
        ""name"": ""different-topic"",
        ""workspaceId"": ""different"",
        ""status"": ""Ready"",
    }
]";

        private string workspaces = @"
[
    {
        ""workspaceId"": ""confluent""
        ,""name"": ""Confluent Kafka Workspace"",
        ""status"": ""Ready"",
        ""broker"": {
            ""address"": ""xxxxx:9092"",
            ""securityMode"": ""SaslSsl"",
            ""sslPassword"": """",
            ""saslMechanism"": ""Plain"",
            ""username"": ""xxxxx"",
            ""password"": ""xxxxx"",
            ""hasCertificate"": false
        },
        ""brokerSettings"": {
            ""brokerType"": ""ConfluentCloud"",
            ""syncTopics"": false,
            ""confluentCloudSettings"": {
                ""clientID"": ""testclientid""
            }
        }
    },
    {
        ""workspaceId"": ""quixdev"",
        ""name"": ""Shared Kafka Workspace"",
        ""status"": ""Ready"",
        ""broker"": {
            ""address"": ""xxxx:9092"",
            ""securityMode"": ""SaslSsl"",
            ""sslPassword"": ""xxxx"",
            ""saslMechanism"": ""ScramSha256"",
            ""username"": ""xxxx"",
            ""password"": ""xxxx"",
            ""hasCertificate"": false
        },
        ""brokerSettings"": {
            ""brokerType"": ""SharedKafka""
        }
    },
    {
        ""workspaceId"": ""different"",
        ""name"": ""Unknown Kafka Workspace"",
        ""status"": ""Ready"",
        ""broker"": {
            ""address"": ""xxxx:9092"",
            ""securityMode"": ""SaslSsl"",
            ""sslPassword"": ""xxxx"",
            ""saslMechanism"": ""ScramSha256"",
            ""username"": ""xxxx"",
            ""password"": ""xxxx"",
            ""hasCertificate"": false
        },
        ""brokerSettings"": {
            ""brokerType"": ""ThisIsNew""
        }
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
}