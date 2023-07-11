using System;

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
        public WorkspaceBrokerType BrokerType { get; set; }

        /// <summary>
        /// Syncronize existing topics in the broker
        /// </summary>
        public bool SyncTopics { get; set; }

        /// <summary>
        /// BrokerQuotas to be applied to multi tenant brokers
        /// </summary>
        public BrokerQuotas Quotas { get; set; }

        /// <summary>
        /// Confluent Cloud settings
        /// </summary>
        public ConfluentCloudSettings ConfluentCloudSettings { get; set; }

        /// <summary>
        /// Amazon Msk settings
        /// </summary>
        public AmazonMskSettings AmazonMskSettings { get; set; }

        /// <summary>
        /// Self Hosted Kafka settings
        /// </summary>
        public SelfHostedKafkaSettings SelfHostedKafkaSettings { get; set; }
    }
    
    
    /// <summary>
    /// Quotas to be applied to the workspace user
    /// </summary>
    internal class BrokerQuotas
    {
        /// <summary>
        /// Max producer byte rate per second
        /// </summary>
        public int? BrokerProducerByteRate { get; set; }
        
        /// <summary>
        /// Max consumer byte rate per second
        /// </summary>
        public int? BrokerConsumerByteRate { get; set; }
        
        /// <summary>
        /// Max percentage of requests
        /// </summary>
        public int? BrokerRequestPercentage { get; set; }
    }

    /// <summary>
    /// Describes Confluent cluster settings
    /// </summary>
    internal class ConfluentCloudSettings
    {
        /// <summary>
        /// Confluent Cloud Api Key
        /// </summary>
        public string ApiKey { get; set; }

        /// <summary>
        /// Confluent Cloud Api Secret
        /// </summary>
        public string ApiSecret { get; set; }

        /// <summary>
        /// Confluent Cloud Cluster ID
        /// </summary>
        public string ClusterID { get; set; }

        /// <summary>
        /// Confluent Cloud Bootstrap server (Broker hostname)
        /// </summary>
        public string BootstrapServer { get; set; }

        /// <summary>
        /// Confluent Cloud REST endpoint
        /// </summary>
        public string RestEndpoint { get; set; }
        
        /// <summary>
        /// Partnership ID
        /// </summary>
        public string ClientID { get; set; }
    }

    internal class AmazonMskSettings
    {
        public string Region { get; set; }
        public AmazonCredentials Credentials { get; set; }

        public string SecretKmsKeyArn { get; set; }

        public string ClusterArn { get; set; }

        public string BrokerList { get; set; }

        public string Zookeeper { get; set; }

        public BrokerSecurityMode SecurityMode { get; set; }
        public BrokerSaslMechanism? SaslMechanism { get; set; }
    }

    /// <summary>
    /// Describes Self Hosted Broker Settings
    /// </summary>
    internal class SelfHostedKafkaSettings
    {
        /// <summary>
        /// List of brokers that can be used for bootstrap-server
        /// and to connect to
        /// </summary>
        public string BrokerList { get; set; }

        /// <summary>
        /// Zookeeper address, optional
        /// </summary>
        public string Zookeeper { get; set; }

        /// <summary>
        /// Security mode to connect to the brokers
        /// </summary>
        public BrokerSecurityMode SecurityMode { get; set; }
        
        /// <summary>
        /// If Sasl is enabled, mechanism to use
        /// </summary>
        public BrokerSaslMechanism? SaslMechanism { get; set; }

        /// <summary>
        /// Username, can be null if authentication is not enabled
        /// </summary>
        public string Username { get; set; }
        
        /// <summary>
        /// Password, can be null if authentication is not enabled
        /// </summary>
        public string Password { get; set; }
        
        /// <summary>
        /// Zip file in base 64, containing a ca.cert file with the
        /// Certificate Authority
        /// </summary>
        public string SslCertificatesBase64 { get; set; }
        
        /// <summary>
        /// Cluster size of self hosted kafka broker.
        /// It will be used for hint as maximum topic replica factor
        /// </summary>
        public int? ClusterSize { get; set; }
    }

    internal class AmazonCredentials
    {
        public string AccessKeyId { get; set; }
        public string SecretAccessKey { get; set; }
    }

}