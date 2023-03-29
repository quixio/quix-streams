namespace QuixStreams.Streaming.Configuration
{
    /// <summary>
    /// Kafka security option for configuring SSL encryption with SASL authentication
    /// </summary>
    public class SecurityOptions
    {
        /// <summary>
        /// The SASL mechanism to use
        /// </summary>
        public SaslMechanism? SaslMechanism { get; set; }
        
        /// <summary>
        /// SASL username.
        /// </summary>
        public string Username { get; set; }
        
        /// <summary>
        /// SASL password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Folder/file that contains the certificate authority certificate(s) to validate the ssl connection.
        /// </summary>
        public string SslCertificates { get; set; }

        /// <summary>
        /// Use SSL
        /// </summary>
        public bool UseSsl { get; set; }

        /// <summary>
        /// Use authentication
        /// </summary>
        public bool UseSasl { get; set; }

        /// <summary>
        /// For deserialization when binding to Configurations like Appsettings
        /// </summary>
        public SecurityOptions()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="SecurityOptions"/> that is configured for SSL encryption with SASL authentication
        /// </summary>
        /// <param name="sslCertificates">The folder/file that contains the certificate authority certificate(s) to validate the ssl connection. Example: "./certificates/ca.cert"</param>
        /// <param name="username">The username for the SASL authentication</param>
        /// <param name="password">The password for the SASL authentication</param>
        /// <param name="saslMechanism">The SASL mechanism to use. Defaulting to ScramSha256 for backward compatibility</param>
        public SecurityOptions(string sslCertificates, string username, string password, SaslMechanism saslMechanism = Configuration.SaslMechanism.ScramSha256)
        {
            this.SslCertificates = sslCertificates;
            this.Username = username;
            this.Password = password;
            this.SaslMechanism = saslMechanism;

            // Assume that if we get sslCertificates it's because we will use ssl
            this.UseSsl = !string.IsNullOrEmpty(this.SslCertificates);

            // Assume that if we have username, we will use Sasl
            this.UseSasl = !string.IsNullOrEmpty(this.Username);
        }
    }
}
