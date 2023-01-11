from .. import __importnet
import Quix.Sdk.Streaming
from .saslmechanism import SaslMechanism


class SecurityOptions(object):
    """Kafka security option for configuring SSL encryption with SASL authentication"""

    def __init__(self, ssl_certificates: str, username: str, password: str, sasl_mechanism: SaslMechanism = SaslMechanism.ScramSha256):
        """
            Create a new instance of SecurityOptions that is configured for SSL encryption with SASL authentication


            Parameters:

            ssl_certificates (string): The folder/file that contains the certificate authority certificate(s) to validate the ssl connection. Example: "./certificates/ca.cert"

            username (string): The username for the SASL authentication

            password (string): The password for the SASL authentication

            sasl_mechanism (SaslMechanism): The SASL mechanism to use. Defaulting to ScramSha256 for backward compatibility
        """

        sasl = SaslMechanism.ScramSha256
        if sasl_mechanism is not None:
            sasl = sasl_mechanism.convert_to_net()
        self.__wrapped = Quix.Sdk.Streaming.Configuration.SecurityOptions(ssl_certificates, username, password, sasl)

    def convert_to_net(self):
        return self.__wrapped

