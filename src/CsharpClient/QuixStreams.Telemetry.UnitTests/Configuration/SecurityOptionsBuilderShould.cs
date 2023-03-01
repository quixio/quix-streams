using System.Collections.Generic;
using Confluent.Kafka;
using FluentAssertions;
using QuixStreams.Telemetry.Configuration;
using Xunit;

namespace QuixStreams.Telemetry.UnitTests.Configuration
{
    public class SecurityOptionsBuilderShould
    {
        [Fact]
        public void SetEncryptionToSSL_ValidValues_ShouldBuildExpected()
        {
            // Act
            var secuConfig = new SecurityOptionsBuilder().SetSslEncryption("./TestCertificates/ca.cert").Build();
           
            // Arrange
            secuConfig.Should().BeEquivalentTo(new Dictionary<string,string>()
            {
                {"ssl.ca.location", "./TestCertificates/ca.cert"},
                {"security.protocol", "SSL"}
            });
        }
        
        [Fact]
        public void SetEncryptionToSASL_SSL_ValidValues_ShouldBuildExpected()
        {
            // Act
            var secuConfig = new SecurityOptionsBuilder().SetSslEncryption("./TestCertificates/ca.cert").SetSaslAuthentication("user", "password", SaslMechanism.ScramSha256).Build();
           
            // Arrange
            secuConfig.Should().BeEquivalentTo(new Dictionary<string,string>()
            {
                {"ssl.ca.location", "./TestCertificates/ca.cert"},
                {"security.protocol", "SASL_SSL"},
                {"sasl.mechanism", "SCRAM-SHA-256"},
                {"sasl.password", "password"},
                {"sasl.username", "user"},
            });
        }
    }
}