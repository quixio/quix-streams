namespace QuixStreams.Streaming.Configuration
{
    /// <summary>
    /// Kafka SASL mechanism
    /// </summary>
    public enum SaslMechanism
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
        OAuthBearer,
    }
}