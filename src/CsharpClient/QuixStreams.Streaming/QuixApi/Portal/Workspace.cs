using System;
using Newtonsoft.Json;

namespace QuixStreams.Streaming.QuixApi.Portal
{
    /// <summary>
    /// Describes properties of a workspace
    /// </summary>
    internal class Workspace
    {
        private string workspaceId;

        /// <summary>
        /// The unique identifier of the workspace
        /// </summary>
        public string WorkspaceId
        {
            get { return this.workspaceId; }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(WorkspaceId));
                this.workspaceId = value.ToLowerInvariant();
            }
        }

        /// <summary>
        /// The display name of the workspace
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The status of the workspace
        /// </summary>
        public WorkspaceStatus Status { get; set; }

        /// <summary>
        /// Broker connection details of the Workspace
        /// </summary>
        public WorkspaceBrokerDetails Broker { get; set; }
        
        /// <summary>
        /// The broker settings
        /// </summary>
        public WorkspaceBrokerSettings BrokerSettings { get; set; }
    }

    /// <summary>
    /// Broker connection details of the Workspace
    /// </summary>
    internal class WorkspaceBrokerDetails
    {
        /// <summary>
        /// Kafka security mode.
        /// </summary>
        public BrokerSecurityMode SecurityMode { get; set; }

        /// <summary>
        /// SASL mechanism (PLAIN, SCRAM-SHA-256, etc)
        /// </summary>
        public BrokerSaslMechanism SaslMechanism { get; set; }

        /// <summary>
        /// SSL password.
        /// </summary>
        public string SslPassword { get; set; }

        /// <summary>
        /// SASL username.
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// SASL password. 
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Address of the broker
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Whether certificate is used for the broker for server or client validation
        /// </summary>
        public bool HasCertificate { get; set; } = true; // assume true unless set otherwise
    }

    /// <summary>
    /// Broker security mode.
    /// </summary>
    internal enum BrokerSecurityMode
    {
        /// <summary>
        /// SSL authorized without ACL.
        /// </summary>
        Ssl,

        /// <summary>
        /// SSL secured ACL role system.
        /// </summary>
        SaslSsl,

        /// <summary>
        /// Plain Text mode
        /// </summary>
        PlainText,

        /// <summary>
        /// ACL role system
        /// </summary>
        Sasl
    }

    /// <summary>
    /// SaslMechanism enum values
    /// </summary>
    internal enum BrokerSaslMechanism
    {
        /// <summary>
        /// GSSAPI
        /// </summary>
        Gssapi,

        /// <summary>
        /// PLAIN
        /// </summary>
        Plain,

        /// <summary>
        /// SCRAM-SHA-256
        /// </summary>
        ScramSha256,

        /// <summary>
        /// SCRAM-SHA-512
        /// </summary>
        ScramSha512,

        /// <summary>
        /// OAUTHBEARER
        /// </summary>
        OAuthBearer
    }

    /// <summary>
    /// The possible statuses of a workspace
    /// </summary>
    internal enum WorkspaceStatus
    {
        /// <summary>
        /// The workspace is currently being created
        /// </summary>
        Creating,

        /// <summary>
        /// The workspace is ready for use
        /// </summary>
        Ready,

        /// <summary>
        /// The workspace is currently being deleted
        /// </summary>
        Deleting,

        /// <summary>
        /// The workspace has encountered an error
        /// </summary>
        Error,

        /// <summary>
        /// The workspace is currently being enabled
        /// </summary>
        Enabling,

        /// <summary>
        /// The workspace is currently being disabled
        /// </summary>
        Disabling,

        /// <summary>
        /// The workspace is disabled
        /// </summary>
        Disabled
    }
    
    internal enum WorkspaceBrokerType
    {
        /// <summary>
        /// Unknown broker type
        /// </summary>
        Unknown,
        
        /// <summary>
        /// Quix shared managed Kafka cluster
        /// </summary>
        SharedKafka,

        /// <summary>
        /// Confluent Cloud Kafka cluster 
        /// </summary>
        ConfluentCloud,

        /// <summary>
        /// Self Hosted Kafka cluster
        /// </summary>
        SelfHosted,

        /// <summary>
        /// Amazon Msk Kafka cluster
        /// </summary>
        AmazonMsk
    }
    
    /// <summary>
    /// Describes the workspace broker settings
    /// </summary>
    internal class WorkspaceBrokerSettings
    {
        /// <summary>
        /// Broker type
        /// </summary>
        [JsonConverter(typeof(WorkspaceBrokerTypeJsonConverter))]
        public WorkspaceBrokerType BrokerType { get; set; }

        /// <summary>
        /// Confluent Cloud settings
        /// </summary>
        public ConfluentCloudSettings ConfluentCloudSettings { get; set; }
    }

    /// <summary>
    /// Describes Confluent cluster settings
    /// </summary>
    internal class ConfluentCloudSettings
    {
        /// <summary>
        /// Partnership ID
        /// </summary>
        public string ClientID { get; set; }
    }
}