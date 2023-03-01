using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;

namespace QuixStreams.Telemetry.Configuration
{
    /// <summary>
    /// Builder for creating valid kafka encryption and authentication configuration
    /// </summary>
    public class SecurityOptionsBuilder
    {
        private EncryptionSelected encryptionSelected = EncryptionSelected.None;
        private AuthenticationSelected authenticationSelected = AuthenticationSelected.None;

        private SslEncryptionConfiguration sslEncryptionConfiguration = null;
        private SaslConfiguration saslConfiguration = null;
        
        /// <summary>
        /// Configures the builder to use SSL encryption
        /// </summary>
        /// <param name="certificatePath">
        /// Sets the certificate authority certificate
        /// </param>
        /// <returns>The builder</returns>
        public SecurityOptionsBuilder SetSslEncryption(string certificatePath = null)
        {
            encryptionSelected = EncryptionSelected.SSL;

            if (certificatePath != null)
            {
                // validate file existence
                sslEncryptionConfiguration = new SslEncryptionConfiguration
                {
                    CaLocation = certificatePath,
                };
                if (!File.Exists(sslEncryptionConfiguration.CaLocation) && !Directory.Exists(sslEncryptionConfiguration.CaLocation)) throw new FileNotFoundException($"certificate or folder not found. CaLocation = '{sslEncryptionConfiguration.CaLocation}'", sslEncryptionConfiguration.CaLocation);
            }

            return this;
        }

        /// <summary>
        /// Configures the builder to use PLAINTEXT (no encryption)
        /// </summary>
        /// <returns>The builder</returns>
        public SecurityOptionsBuilder SetNoEncryption()
        {
            encryptionSelected = EncryptionSelected.None;
            return this;
        }

        /// <summary>
        /// Configures the builder to use SASL authentication
        /// </summary>
        /// <param name="username">the username to use</param>
        /// <param name="password">the password to use</param>
        /// <param name="saslMechanism">The SASL mechanism</param>
        /// <returns>The builder</returns>
        public SecurityOptionsBuilder SetSaslAuthentication(string username, string password, SaslMechanism saslMechanism)
        {
            authenticationSelected = AuthenticationSelected.SASL;
            saslConfiguration = new SaslConfiguration
            {
                Password = password,
                Username = username,
                SaslMechanism = saslMechanism
            };
            return this;
        }

        /// <summary>
        /// Configures the builder to not use any authentication
        /// </summary>
        /// <returns>The builder</returns>
        public SecurityOptionsBuilder SetNoAuthentication()
        {
            authenticationSelected = AuthenticationSelected.None;
            saslConfiguration = null;
            return this;
        }

        private class SslEncryptionConfiguration
        {
            public string CaLocation { get; set; }
        }

        private class SaslConfiguration
        {
            public string Username { get; set; }
            public string Password { get; set; }
            
            public SaslMechanism SaslMechanism { get; set; }
        }

        private enum EncryptionSelected
        {
            None,
            SSL
        }
        
        private enum AuthenticationSelected 
        {
            None,
            SASL,
            //SSL // TODO?
        }

        /// <summary>
        /// Builds the kafka configuration dictionary
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public Dictionary<string, string> Build()
        {
            Dictionary<string,string> kafkaConfiguration = new Dictionary<string, string>();
            switch (this.encryptionSelected)
            {
                case EncryptionSelected.None:
                    switch (authenticationSelected)
                    {
                        case AuthenticationSelected.None:
                            kafkaConfiguration["security.protocol"] = "PLAINTEXT";
                            break;
                        case AuthenticationSelected.SASL:
                            kafkaConfiguration["security.protocol"] = "SASL_PLAINTEXT";
                            SetSaslDetails();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Security {this.encryptionSelected} is not handled in combination with {authenticationSelected}");
                    }
                    break;
                case EncryptionSelected.SSL:
                    switch (authenticationSelected)
                    {
                        case AuthenticationSelected.None:
                            kafkaConfiguration["security.protocol"] = "SSL";
                            SetSslEncryptionDetails();
                            break;
                        case AuthenticationSelected.SASL:
                            kafkaConfiguration["security.protocol"] = "SASL_SSL";
                            SetSaslDetails();
                            SetSslEncryptionDetails();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Security {this.encryptionSelected} is not handled in combination with {authenticationSelected}");
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Security {this.encryptionSelected} is not handled");
            }

            void SetSaslDetails()
            {
                kafkaConfiguration["sasl.username"] = saslConfiguration.Username;
                kafkaConfiguration["sasl.password"] = saslConfiguration.Password;
                switch (saslConfiguration.SaslMechanism)
                {
                    case SaslMechanism.Gssapi:
                        kafkaConfiguration["sasl.mechanism"] = "GSSAPI";
                        break;
                    case SaslMechanism.Plain:
                        kafkaConfiguration["sasl.mechanism"] = "PLAIN";
                        break;
                    case SaslMechanism.ScramSha256:
                        kafkaConfiguration["sasl.mechanism"] = "SCRAM-SHA-256";
                        break;
                    case SaslMechanism.ScramSha512:
                        kafkaConfiguration["sasl.mechanism"] = "SCRAM-SHA-512";
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($"SASL mechanism {saslConfiguration.SaslMechanism} is not supported");
                }
            }

            void SetSslEncryptionDetails()
            {
                if (sslEncryptionConfiguration != null)
                {
                    kafkaConfiguration["ssl.ca.location"] = sslEncryptionConfiguration.CaLocation;
                }
            }

            return kafkaConfiguration;
        }
    }
}