using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using QuixStreams;
using QuixStreams.Kafka.Transport;
using QuixStreams.Streaming.Configuration;
using QuixStreams.Streaming.Exceptions;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.QuixApi;
using QuixStreams.Streaming.QuixApi.Portal;
using QuixStreams.Streaming.QuixApi.Portal.Requests;
using QuixStreams.Streaming.Raw;
using QuixStreams.Streaming.Utils;
using QuixStreams.Telemetry.Kafka;

namespace QuixStreams.Streaming
{
    public interface IQuixStreamingClient
    {
        /// <summary>
        /// Gets a topic consumer capable of subscribing to receive incoming streams.
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="options">The settings to use for committing</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        ITopicConsumer GetTopicConsumer(string topicIdOrName, string consumerGroup = null, CommitOptions options = null, AutoOffsetReset autoOffset = AutoOffsetReset.Latest);

        /// <summary>
        /// Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. 
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="IRawTopicConsumer"/></returns>
        IRawTopicConsumer GetRawTopicConsumer(string topicIdOrName, string consumerGroup = null, AutoOffsetReset? autoOffset = null);

        /// <summary>
        /// Gets a topic producer capable of publishing non-quixstreams messages. 
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <returns>Instance of <see cref="IRawTopicProducer"/></returns>
        IRawTopicProducer GetRawTopicProducer(string topicIdOrName);

        /// <summary>
        /// Gets a topic producer capable of publishing stream messages.
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <returns>Instance of <see cref="ITopicProducer"/></returns>
        ITopicProducer GetTopicProducer(string topicIdOrName);
    }

    /// <summary>
    /// Streaming client for Kafka configured automatically using Environment Variables and Quix platform endpoints.
    /// Use this Client when you use this library together with Quix platform.
    /// </summary>
    public class QuixStreamingClient : IQuixStreamingClient
    {
        private readonly ILogger logger = QuixStreams.Logging.CreateLogger<QuixStreamingClient>();
        private readonly IDictionary<string, string> brokerProperties;
        private readonly string token;
        private readonly bool autoCreateTopics;
        private readonly bool debug;
        private readonly ConcurrentDictionary<string, KafkaStreamingClient> wsToStreamingClientDict = new ConcurrentDictionary<string, KafkaStreamingClient>();
        private readonly ConcurrentDictionary<string, Workspace> topicToWorkspaceDict = new ConcurrentDictionary<string, Workspace>();
        private Lazy<HttpClient> httpClient;
        private HttpClientHandler handler;
        private const string WorkspaceIdEnvironmentKey = "Quix__Workspace__Id";
        private const string PortalApiEnvironmentKey = "Quix__Portal__Api";
        private const string SdkTokenKey = "Quix__Sdk__Token";
        private static readonly ConcurrentDictionary<string, object> workspaceLocks = new ConcurrentDictionary<string, object>();

        /// <summary>
        /// The base API uri. Defaults to <c>https://portal-api.platform.quix.ai</c>, or environment variable <c>Quix__Portal__Api</c> if available.
        /// </summary>
        public Uri ApiUrl = new Uri("https://portal-api.platform.quix.ai");
        
        /// <summary>
        /// The period for which some API responses will be cached to avoid excessive amount of calls. Defaults to 1 minute.
        /// </summary>
        public TimeSpan CachePeriod = TimeSpan.FromMinutes(1);

        private bool tokenChecked;

        /// <summary>
        /// Gets or sets the token validation configuration
        /// </summary>
        public TokenValidationConfiguration TokenValidationConfig { get; set; } = new TokenValidationConfiguration();

        /// <summary>
        /// Initializes a new instance of <see cref="KafkaStreamingClient"/> that is capable of creating topic consumer and producers
        /// </summary>
        /// <param name="token">The token to use when talking to Quix. When not provided, Quix__Sdk__Token environment variable will be used</param>
        /// <param name="autoCreateTopics">Whether topics should be auto created if they don't exist yet</param>
        /// <param name="properties">Additional broker properties</param>
        /// <param name="debug">Whether debugging should be enabled</param>
        /// <param name="httpClient">The http client to use</param>
        public QuixStreamingClient(string token = null, bool autoCreateTopics = true, IDictionary<string, string> properties = null, bool debug = false, HttpClient httpClient = null)
        {
            this.token = token;
            if (string.IsNullOrWhiteSpace(this.token))
            {
                this.token = Environment.GetEnvironmentVariable(SdkTokenKey);
                if (string.IsNullOrWhiteSpace(this.token))
                {
                    throw new InvalidConfigurationException($"Token must be given as an argument or set in {SdkTokenKey} environment variable.");
                }
                this.logger.LogTrace("Using token from environment variable {1}", SdkTokenKey);
            }
            this.autoCreateTopics = autoCreateTopics;
            this.brokerProperties = properties ?? new Dictionary<string, string>();
            this.debug = debug;
            if (httpClient == null)
            {
                this.handler = new HttpClientHandler();
                
                httpClient = new HttpClient(handler);
            }

            this.httpClient = new Lazy<HttpClient>(() => httpClient);
            
            var envUri = Environment.GetEnvironmentVariable(PortalApiEnvironmentKey)?.ToLowerInvariant().TrimEnd('/');
            if (!string.IsNullOrWhiteSpace(envUri))
            {
                if (envUri != ApiUrl.AbsoluteUri.ToLowerInvariant().TrimEnd('/'))
                {
                    this.logger.LogInformation("Using {0} endpoint for portal, configured from env var {1}", envUri, PortalApiEnvironmentKey);
                    ApiUrl = new Uri(envUri);
                }
            }
        }

        /// <summary>
        /// Gets a topic consumer capable of subscribing to receive incoming streams.
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="options">The settings to use for committing</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public ITopicConsumer GetTopicConsumer(string topicIdOrName, string consumerGroup = null, CommitOptions options = null, AutoOffsetReset autoOffset = AutoOffsetReset.Latest)
        {
            if (string.IsNullOrWhiteSpace(topicIdOrName)) throw new ArgumentNullException(nameof(topicIdOrName));
            
            var (client, topicId, ws) = this.ValidateTopicAndCreateClient(topicIdOrName).ConfigureAwait(false).GetAwaiter().GetResult();
            (consumerGroup, options) = GetValidConsumerGroup(topicIdOrName, consumerGroup, options).ConfigureAwait(false).GetAwaiter().GetResult();
            
            return client.GetTopicConsumer(topicId, consumerGroup, options, autoOffset);
        }

        /// <summary>
        /// Gets a topic consumer capable of subscribing to receive non-quixstreams incoming messages. 
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <returns>Instance of <see cref="IRawTopicConsumer"/></returns>
        public IRawTopicConsumer GetRawTopicConsumer(string topicIdOrName, string consumerGroup = null, AutoOffsetReset? autoOffset = null)
        {
            if (string.IsNullOrWhiteSpace(topicIdOrName)) throw new ArgumentNullException(nameof(topicIdOrName));

            var (client, topicId, _) = this.ValidateTopicAndCreateClient(topicIdOrName).ConfigureAwait(false).GetAwaiter().GetResult();
            (consumerGroup, _) = GetValidConsumerGroup(topicIdOrName, consumerGroup, null).ConfigureAwait(false).GetAwaiter().GetResult();

            return client.GetRawTopicConsumer(topicId, consumerGroup, autoOffset);
        }

        /// <summary>
        /// Gets a topic producer capable of publishing non-quixstreams messages. 
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <returns>Instance of <see cref="IRawTopicProducer"/></returns>
        public IRawTopicProducer GetRawTopicProducer(string topicIdOrName)
        {
            if (string.IsNullOrWhiteSpace(topicIdOrName)) throw new ArgumentNullException(nameof(topicIdOrName));

            var (client, topicId, _) = this.ValidateTopicAndCreateClient(topicIdOrName).ConfigureAwait(false).GetAwaiter().GetResult();

            return client.GetRawTopicProducer(topicId);
        }
        
        /// <summary>
        /// Gets a topic producer capable of publishing stream messages.
        /// </summary>
        /// <param name="topicIdOrName">Id or name of the topic. If name is provided, workspace will be derived from environment variable or token, in that order</param>
        /// <returns>Instance of <see cref="ITopicProducer"/></returns>
        public ITopicProducer GetTopicProducer(string topicIdOrName)
        {
            if (string.IsNullOrWhiteSpace(topicIdOrName)) throw new ArgumentNullException(nameof(topicIdOrName));

            var (client, topicId, _) = this.ValidateTopicAndCreateClient(topicIdOrName).ConfigureAwait(false).GetAwaiter().GetResult();

            return client.GetTopicProducer(topicId);
        }

        private async Task<(string, CommitOptions)> GetValidConsumerGroup(string topicIdOrName, string originalConsumerGroup, CommitOptions commitOptions)
        {
            topicIdOrName = topicIdOrName.Trim();
            originalConsumerGroup = originalConsumerGroup?.Trim();
            var newCommitOptions = commitOptions;
            var consumerGroup = originalConsumerGroup;
            
            var ws = await GetWorkspaceFromConfiguration(topicIdOrName);
            if (originalConsumerGroup == null)
            {
                if (commitOptions?.AutoCommitEnabled == true)
                {
                    this.logger.LogWarning("Disabling commit options as no consumer group is set. To remove this warning, set consumer group or disable auto commit.");
                }

                newCommitOptions = new CommitOptions()
                {
                    AutoCommitEnabled = false
                };
                
                // Hacky workaround to an issue that Kafka client can't be left with no GroupId, but it still uses it for ACL checks.
                QuixStreams.Kafka.ConsumerConfiguration.ConsumerGroupIdWhenNotSet = ws.WorkspaceId + "-" + Guid.NewGuid().ToString("N").Substring(0, 10);
                return (null, newCommitOptions);
            }
            
            if (!consumerGroup.StartsWith(ws.WorkspaceId))
            {
                this.logger.LogDebug("Updating consumer group to have workspace id prefix to avoid being invalid.");
                consumerGroup = ws.WorkspaceId + "-" + consumerGroup;
            }

            return (consumerGroup, newCommitOptions);
        }

        private async Task<(KafkaStreamingClient client, string topicId, Workspace ws)> ValidateTopicAndCreateClient(string topicIdOrName)
        {
            CheckToken(token);
            topicIdOrName = topicIdOrName.Trim();
            var sw = Stopwatch.StartNew();
            var ws = await GetWorkspaceFromConfiguration(topicIdOrName);
            var client = await this.CreateStreamingClientForWorkspace(ws);
            sw.Stop();
            this.logger.LogTrace("Created streaming client for workspace {0} in {1}.", ws.WorkspaceId, sw.Elapsed);

            sw = Stopwatch.StartNew();
            var topicId = await this.ValidateTopicExistence(ws, topicIdOrName);
            sw.Stop();
            this.logger.LogTrace("Validated topic {0} in {1}.", topicIdOrName, sw.Elapsed);
            return (client, topicId, ws);
        }

        /// <summary>
        /// Validate that topic exists within the workspace and create it if it doesn't
        /// </summary>
        /// <param name="workspace">Workspace</param>
        /// <param name="topicIdOrName">Topic Id or Topic Name</param>
        /// <returns>Topic Id</returns>
        private async Task<string> ValidateTopicExistence(Workspace workspace, string topicIdOrName)
        {
            this.logger.LogTrace("Checking if topic {0} is already created.", topicIdOrName);
            var topics = await this.GetTopics(workspace, true);
            var existingTopic = topics.FirstOrDefault(y => y.Id.Equals(topicIdOrName, StringComparison.InvariantCulture)) ?? topics.FirstOrDefault(y=> y.Name.Equals(topicIdOrName, StringComparison.InvariantCulture)); // id prio
            var topicName = existingTopic?.Name;
            if (topicName == null)
            {
                if (topicIdOrName.StartsWith(workspace.WorkspaceId, StringComparison.InvariantCultureIgnoreCase))
                {
                    var before = topicIdOrName;
                    topicName = new Regex($"^{workspace.WorkspaceId}\\-", RegexOptions.IgnoreCase).Replace(topicIdOrName, "");
                    if (topicName == before) throw new ArgumentException($"The specified topic id {topicIdOrName} is malformed.");
                    if (string.IsNullOrWhiteSpace(topicName)) throw new ArgumentException($"The specified topic id {topicIdOrName} is missing topic name part.");
                }
                else topicName = topicIdOrName;
            }
            
            if (existingTopic == null)
            {
                if (!this.autoCreateTopics)
                {
                    throw new InvalidConfigurationException($"Topic {topicIdOrName} does not exist and configuration is set to not automatically create.");
                }
                this.logger.LogInformation("Topic {0} is not yet created, creating in workspace {1} due to active settings.", topicName, workspace.WorkspaceId);
                existingTopic = await CreateTopic(workspace, topicName);
                this.logger.LogTrace("Created topic {0}.", topicName);
            }
            else
            {
                this.logger.LogTrace("Topic {0} is already created.", topicName);
            }
            
            while (existingTopic.Status == TopicStatus.Creating)
            {
                this.logger.LogInformation("Topic {0} is still creating.", topicName);
                await Task.Delay(1000);
                existingTopic = await this.GetTopic(workspace, topicName, false);
                if (existingTopic.Status == TopicStatus.Ready) this.logger.LogInformation("Topic {0} created.", topicName); // will break out by itself
            }

            switch (existingTopic.Status)
            {
                case TopicStatus.Deleting:
                    throw new InvalidConfigurationException("Topic {0} is being deleted, not able to use.");
                case TopicStatus.Ready:
                    this.logger.LogDebug("Topic {0} is available for streaming.", topicName);
                    break;
                default:
                    this.logger.LogWarning("Topic {0} is in state {1}, but expected {2}.", topicName, existingTopic.Status, TopicStatus.Ready);
                    break;
            }

            return existingTopic.Id;
        }

        private async Task<Workspace> GetWorkspaceFromConfiguration(string topicIdOrName)
        {
            var workspaces = await this.GetWorkspaces();
            
            // Assume it is an ID
            if (topicToWorkspaceDict.TryGetValue(topicIdOrName, out var ws))
            {
                this.logger.LogTrace("Retrieving workspace for topic {0} from cache", topicIdOrName);
                return ws;
            }

            this.logger.LogTrace("Checking if workspace matching topic {0} exists", topicIdOrName);
            var matchingWorkspace = workspaces.OrderBy(y => y.WorkspaceId.Length).FirstOrDefault(y => topicIdOrName.StartsWith(y.WorkspaceId + '-'));
            if (matchingWorkspace != null)
            {
                this.logger.LogTrace("Found workspace using topic id where topic {0} can be present, called {1}.", topicIdOrName, matchingWorkspace.Name);

                return topicToWorkspaceDict.GetOrAdd(topicIdOrName, matchingWorkspace);
            }
            
            // If a single workspace is available, then lets use that
            if (workspaces.Count == 1)
            {
                matchingWorkspace = workspaces.First();
                this.logger.LogTrace("Found workspace using token where topic {0} can be present, called {1}.", topicIdOrName, matchingWorkspace.Name);
                return topicToWorkspaceDict.GetOrAdd(topicIdOrName, matchingWorkspace);
            }
            
            // Assume it is a name, in which case the workspace comes from environment variables or token
            // Environment variable check
            var envWs = Environment.GetEnvironmentVariable(WorkspaceIdEnvironmentKey);
            if (!string.IsNullOrWhiteSpace(envWs))
            {
                matchingWorkspace = workspaces.FirstOrDefault(y => y.WorkspaceId == envWs);
                if (matchingWorkspace != null)
                {
                    this.logger.LogTrace("Found workspace using environment variable where topic {0} can be present, called {1}.", topicIdOrName, matchingWorkspace.Name);
                    return topicToWorkspaceDict.GetOrAdd(topicIdOrName, matchingWorkspace);
                }
                
                var almostWorkspaceByEnv = workspaces.FirstOrDefault(y => y.WorkspaceId.GetLevenshteinDistance(envWs) < 2);
                if (almostWorkspaceByEnv != null)
                {
                    throw new InvalidConfigurationException($"The workspace id specified ({envWs}) in environment variable {WorkspaceIdEnvironmentKey} is similar to {almostWorkspaceByEnv.WorkspaceId}, but not exact. Typo or token without access to it?");
                }
                throw new InvalidConfigurationException($"The workspace id specified ({envWs}) in environment variable {WorkspaceIdEnvironmentKey} is not available. Typo or token without access to it?");
            }
            
            // Not able to find workspace, lets figure out what kind of exception we throw back. These exceptions are about topic id/name being similar/invalid, other methods have their exception above
            var exactWorkspace = workspaces.FirstOrDefault(y => y.WorkspaceId == topicIdOrName);
            if (exactWorkspace != null)
            {
                throw new InvalidConfigurationException($"The specified topic id {topicIdOrName} is missing the topic name. Found workspace {exactWorkspace.WorkspaceId}.");
            }
            
            var almostWorkspace = workspaces.FirstOrDefault(y => y.WorkspaceId.GetLevenshteinDistance(topicIdOrName) < 2);
            if (almostWorkspace != null)
            {
                throw new InvalidConfigurationException($"The specified topic id {topicIdOrName} is missing the topic name. Workspace not found either, did you mean {almostWorkspace.WorkspaceId}?");
            }
            
            List<string> possibleWorkspaces;
            if (QuixUtils.TryGetWorkspaceIdPrefix(topicIdOrName, out var wsId))
            {
                possibleWorkspaces = wsId.GetLevenshteinDistance(workspaces.Select(y => y.WorkspaceId), 3).ToList();
            }
            else
            {
                var lastDash = topicIdOrName.LastIndexOf('-');
                var possibleTopicName = topicIdOrName.Substring(lastDash > -1 ? lastDash : topicIdOrName.Length);
                var possibleWsId = topicIdOrName.Substring(0, topicIdOrName.Length - possibleTopicName.Length);
                possibleWorkspaces = possibleWsId.GetLevenshteinDistance(workspaces.Select(y => y.WorkspaceId), 5).ToList();
            }

            if (possibleWorkspaces.Count > 0)
            {
                throw new InvalidConfigurationException($"Could not find the workspace for topic {topicIdOrName}. Verify that the workspace exists, token provided has access to it or use {nameof(KafkaStreamingClient)} instead of {nameof(QuixStreamingClient)}. Similar workspaces found: {string.Join(", ", possibleWorkspaces)}");

            }

            throw new InvalidConfigurationException($"No workspace could be identified for topic {topicIdOrName}. Verify the topic id or name is correct. If name is provided then {WorkspaceIdEnvironmentKey} environment variable or token with access to 1 workspace only must be provided. Current token has access to {workspaces.Count} workspaces and env var is unset. Alternatively use {nameof(KafkaStreamingClient)} instead of {nameof(QuixStreamingClient)}.");
        }

        private async Task<KafkaStreamingClient> CreateStreamingClientForWorkspace(Workspace ws)
        {
            if (this.wsToStreamingClientDict.TryGetValue(ws.WorkspaceId, out var sc))
            {
                return sc;
            }
            
            if (ws.Broker == null)
            {
                throw new InvalidConfigurationException("Unable to configure broker due missing credentials.");
            }
            
            if (ws.Status != WorkspaceStatus.Ready)
            {
                if (ws.Status == WorkspaceStatus.Deleting)
                {
                    throw new InvalidConfigurationException($"Workspace {ws.WorkspaceId} is being deleted.");
                }
                logger.LogWarning("Workspace {0} is in state {1} instead of {2}.", ws.WorkspaceId, ws.Status, WorkspaceStatus.Ready);
            }

            var securityOptions = new SecurityOptions();

            if (ws.Broker.SecurityMode == BrokerSecurityMode.Ssl || ws.Broker.SecurityMode == BrokerSecurityMode.SaslSsl)
            {
                securityOptions.UseSsl = true;
                securityOptions.SslCertificates = await GetWorkspaceCertificatePath(ws);
                if (!brokerProperties.ContainsKey("ssl.endpoint.identification.algorithm"))
                {
                    brokerProperties["ssl.endpoint.identification.algorithm"] = "none"; // default back to None
                }
            }
            else
            {
                securityOptions.UseSsl = false;
            }

            if (ws.Broker.SecurityMode == BrokerSecurityMode.Sasl || ws.Broker.SecurityMode == BrokerSecurityMode.SaslSsl)
            {
                if (!Enum.TryParse(ws.Broker.SaslMechanism.ToString(), true, out SaslMechanism parsed))
                {
                    throw new ArgumentOutOfRangeException(nameof(ws.Broker.SaslMechanism), "Unsupported sasl mechanism " + ws.Broker.SaslMechanism);
                }

                securityOptions.UseSasl = true;
                securityOptions.SaslMechanism = parsed;
                securityOptions.Username = ws.Broker.Username;
                securityOptions.Password = ws.Broker.Password;
            } 
            else
            {
                securityOptions.UseSasl = false;
            }

            try
            {
                if (ws.BrokerSettings?.BrokerType == WorkspaceBrokerType.ConfluentCloud &&
                    !string.IsNullOrWhiteSpace(ws.BrokerSettings.ConfluentCloudSettings.ClientID))
                {
                    brokerProperties["client.id"] = ws.BrokerSettings.ConfluentCloudSettings.ClientID;
                }
            }
            catch (Exception ex)
            {
                this.logger.LogTrace(ex, "Failed to set Confluent client id");
            }

            var client = new KafkaStreamingClient(ws.Broker.Address, securityOptions, brokerProperties, debug);
            return wsToStreamingClientDict.GetOrAdd(ws.WorkspaceId, client);
        }

        private async Task<string> GetWorkspaceCertificatePath(Workspace ws)
        {
            if (!ws.Broker.HasCertificate) return null;
            var targetFolder = Path.Combine(Directory.GetCurrentDirectory(), "certificates", ws.WorkspaceId);
            var certPath = Path.Combine(targetFolder, "ca.cert");
            if (!File.Exists(certPath))
            {
                var wsLock = workspaceLocks.GetOrAdd(ws.WorkspaceId, new object());
                lock (wsLock)
                {
                    if (!File.Exists(certPath))
                    {
                        async Task<string> HelperFunc()
                        {
                            Directory.CreateDirectory(targetFolder);
                            this.logger.LogTrace("Certificate is not yet downloaded for workspace {0}.", ws.Name);
                            var zipPath = Path.Combine(targetFolder, "certs.zip");
                            if (!File.Exists(zipPath))
                            {
                                this.logger.LogTrace("Downloading certificate for workspace {0}.", ws.Name);
                                var response = await this.SendRequestToApi(HttpMethod.Get, new Uri(ApiUrl, $"workspaces/{ws.WorkspaceId}/certificates"));
                                if (response.StatusCode == HttpStatusCode.NoContent)
                                {
                                    ws.Broker.HasCertificate = false;
                                    return null;
                                }

                                using var fs = File.Open(zipPath, FileMode.Create);
                                await response.Content.CopyToAsync(fs);
                            }

                            var hasCert = false;

                            using (var file = File.OpenRead(zipPath))
                            using (var zip = new ZipArchive(file, ZipArchiveMode.Read))
                            {
                                foreach (var entry in zip.Entries)
                                {
                                    if (entry.Name != "ca.cert") continue;
                                    using var stream = entry.Open();
                                    using var fs = File.Open(certPath, FileMode.Create);
                                    await stream.CopyToAsync(fs);
                                    hasCert = true;
                                }
                            }
                            File.Delete(zipPath);
                            this.logger.LogTrace("Certificate is now available for workspace {0}", ws.Name);
                            if (!hasCert)
                            {
                                this.logger.LogWarning("Expected to find certificate for workspace {0}, but the downloaded zip had none.", ws.Name);
                                return null;
                            }
                            return certPath;
                        }
                        return HelperFunc().ConfigureAwait(true).GetAwaiter().GetResult();
                    }
                    this.logger.LogTrace("Certificate is downloaded by another thread for workspace {0}", ws.Name);
                }
            }
            else
            {
                this.logger.LogTrace("Certificate is already available for workspace {0}", ws.Name);
            }

            return certPath;
        }

        private async Task<List<Workspace>> GetWorkspaces()
        {
            var result = await GetModelFromApi<List<Workspace>>("workspaces", true, true);
            if (result.Count == 0) throw new InvalidTokenException("Could not find any workspaces for this token.");
            return result;
        }
        
        private Task<List<Topic>> GetTopics(Workspace workspace, bool useCache)
        {
            return GetModelFromApi<List<Topic>>($"{workspace.WorkspaceId}/topics", true, useCache);
        }
        
        private Task<Topic> GetTopic(Workspace workspace, string topicName, bool useCache)
        {
            return GetModelFromApi<Topic>($"{workspace.WorkspaceId}/topics/{topicName}", true, useCache);
        }
        
        private async Task<Topic> CreateTopic(Workspace workspace, string topicName)
        {
            var uri = new Uri(ApiUrl, $"{workspace.WorkspaceId}/topics");
            
            
            HttpResponseMessage response;
            try
            {
                response = await SendRequestToApi(HttpMethod.Post, uri, new TopicCreateRequest() {Name = topicName});
            }
            catch (QuixApiException ex) when (ex.Message.Contains("already exists"))
            {
                this.logger.LogDebug("Another process created topic {0}, retrieving it from workspace.", topicName);
                return await this.GetTopic(workspace, topicName, false);
            }

            var content = await response.Content.ReadAsStringAsync();
            try
            {
                var converted = JsonConvert.DeserializeObject<Topic>(content);
                return converted;
            }
            catch
            {
                throw new QuixApiException(uri.AbsolutePath, $"Failed to serialize response while creating topic {topicName}.", String.Empty, HttpStatusCode.OK);
            }
        }

        private async Task<HttpResponseMessage> SendRequestToApi(HttpMethod method, Uri uri, object bodyModel = null)
        {
            var httpRequest = new HttpRequestMessage(method, uri);
            if (bodyModel != null)
            {
                httpRequest.Content = new StringContent(JsonConvert.SerializeObject(bodyModel), Encoding.UTF8, "application/json");
            }
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            try
            {
                var response =
                    await this.httpClient.Value.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead);
                if (!response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    this.logger.LogTrace("Request {0} had {1} response:{2}{3}", uri.AbsoluteUri, response.StatusCode,
                        Environment.NewLine, responseContent);
                    string msg;
                    var cid = String.Empty;
                    try
                    {
                        var error = JsonConvert.DeserializeObject<QuixApi.Portal.PortalException>(responseContent);
                        msg = error.Message;
                        cid = error.CorrelationId;
                    }
                    catch
                    {
                        msg = responseContent;
                    }

                    throw new QuixApiException(uri.AbsoluteUri, msg, cid, response.StatusCode);
                }

                return response;
            }
            catch (HttpRequestException ex) when (ex.InnerException?.Message.Contains("Could not create SSL/TLS secure channel") == true &&
                                                  this.handler?.SslProtocols == SslProtocols.None)
            {
                // This is here for rare compatibility issue when for some reason the library is unable to pick an SSL protocol based on OS configuration
                // So far only seen it happen when running on windows the lib wrapped in python->pythonnet->lib manner. 
                this.logger.LogDebug("Could not create SSL/TLS secure channel for request to {0}, Forcing TLS 1.2", uri.AbsoluteUri);
                this.handler = new HttpClientHandler()
                {
                    SslProtocols = SslProtocols.Tls12
                };
                var prevClient = this.httpClient;
                this.httpClient = new Lazy<HttpClient>(() => new HttpClient(this.handler));
                prevClient.Value.Dispose();
                return await SendRequestToApi(method, uri, bodyModel);
            }
        }
        
        

        private async Task<T> GetModelFromApi<T>(string path, bool saveToCache, bool readFromCache) where T : class
        {
            var uri = new Uri(ApiUrl, path);
            var cacheKey = $"API:GET:{uri.AbsolutePath}";

            var response = await SendRequestToApi(HttpMethod.Get, uri);
            var responseContent = await response.Content.ReadAsStringAsync();
            this.logger.LogTrace("Request {0} had {1} response:{2}{3}", uri.AbsolutePath, response.StatusCode, Environment.NewLine, responseContent);
            try
            {
                var converted = JsonConvert.DeserializeObject<T>(responseContent);

                return converted;
            }
            catch
            {
                throw new QuixApiException(uri.AbsolutePath, "Failed to serialize response.", String.Empty, HttpStatusCode.OK);
            }
        }
        

        private void CheckToken(string token)
        {
            if (string.IsNullOrWhiteSpace(token)) throw new ArgumentNullException(nameof(token));
            if (this.tokenChecked) return;
            this.tokenChecked = true;
            


            if (TokenValidationConfig == null || !TokenValidationConfig.Enabled) return;
            
            var handler = new JwtSecurityTokenHandler();
            if (!handler.CanReadToken(token))
            {
                // Possibly a quix token
                return;
            }
            
            JwtSecurityToken jwt;
            try
            {
                jwt = handler.ReadJwtToken(token);
            }
            catch
            {
                this.logger.LogWarning("Provided token is not a valid JWT token. It will probably not function.");
                return;
            }

            if (DateTime.UtcNow <= jwt.ValidFrom)
            {
                this.logger.LogWarning($"Provided token is only valid from {jwt.ValidFrom}. It will probably not function yet.");
                return;
            }
            
            
            var expires = jwt.ValidTo;
            if (jwt.Payload != null
                && jwt.Payload.TryGetValue("https://quix.ai/exp", out var customExpiry)
                && customExpiry is string value
                && long.TryParse(value, out var customExpirySeconds))
            {
                expires = new DateTime(1970, 01, 01, 0, 0, 0, 0, DateTimeKind.Utc).AddSeconds(customExpirySeconds);
            }
            
            if (DateTime.UtcNow > expires)
            {
                this.logger.LogWarning("Provided token expired at {0}. It will probably not function.", expires);
                return;
            }
            
            if (TokenValidationConfig.WarningBeforeExpiry.HasValue && DateTime.UtcNow.Add(TokenValidationConfig.WarningBeforeExpiry.Value) > expires)
            {
                this.logger.LogWarning("Provided token will expire soon, at {0}. Consider replacing it. You can disable this warning in {1}.", expires, nameof(TokenValidationConfig));
                return;
            }
            
            if (TokenValidationConfig.WarnAboutNonPatToken && jwt.Payload != null && !jwt.Payload.Sub.StartsWith(jwt.Payload.Azp))
            {
                this.logger.LogWarning("Provided token is not a PAT token. Consider replacing it for non-development environments. You can disable this warning in {1}.", nameof(TokenValidationConfig));
                return;
            }

            this.logger.LogTrace($"Provided token passed sanity checks.");
        }

        /// <summary>
        /// Token Validation configuration
        /// </summary>
        public class TokenValidationConfiguration
        {
            /// <summary>
            /// Whether token validation and warnings are enabled. Defaults to <c>true</c>.
            /// </summary>
            public bool Enabled = true;
            
            /// <summary>
            /// If the token expires within this period, a warning will be displayed. Defaults to <c>2 days</c>. Set to null to disable the check
            /// </summary>
            public TimeSpan? WarningBeforeExpiry = TimeSpan.FromDays(2);
            
            /// <summary>
            /// Whether to warn if the provided token is not PAT token. Defaults to <c>true</c>.
            /// </summary>
            public bool WarnAboutNonPatToken = true;
        }
    }

    /// <summary>
    /// Quix Streaming Client extensions
    /// </summary>
    public static class QuixStreamingClientExtensions
    {
        /// <summary>
        /// Gets a topic consumer capable of subscribing to receive streams in the specified topic
        /// </summary>
        /// <param name="client">Quix Streaming client instance</param>
        /// <param name="topicId">Id of the topic. Should look like: myvery-myworkspace-mytopic</param>
        /// <param name="autoOffset">The offset to use when there is no saved offset for the consumer group.</param>
        /// <param name="consumerGroup">The consumer group id to use for consuming messages. If null, consumer group is not used and only consuming new messages.</param>
        /// <param name="commitMode">The commit strategy to use for this topic</param>
        /// <returns>Instance of <see cref="ITopicConsumer"/></returns>
        public static ITopicConsumer GetTopicConsumer(this IQuixStreamingClient client, string topicId, string consumerGroup = null, CommitMode commitMode = CommitMode.Automatic, AutoOffsetReset autoOffset =  AutoOffsetReset.Latest)
        {
            switch (commitMode)
            {
                case CommitMode.Automatic:
                    return client.GetTopicConsumer(topicId, consumerGroup, null, autoOffset);
                case CommitMode.Manual:
                    var commitOptions = new CommitOptions()
                    {
                        AutoCommitEnabled = false
                    };
                    return client.GetTopicConsumer(topicId, consumerGroup, commitOptions, autoOffset);
                default:
                    throw new ArgumentOutOfRangeException(nameof(commitMode), commitMode, null);
            }
        }
    }
}
