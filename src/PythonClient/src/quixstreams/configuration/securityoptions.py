from .saslmechanism import SaslMechanism
from ..helpers.enumconverter import EnumConverter as ec
from ..native.Python.QuixStreamsStreaming.Configuration.SaslMechanism import SaslMechanism as SaslMechanismInterop
from ..native.Python.QuixStreamsStreaming.Configuration.SecurityOptions import SecurityOptions as soi


class SecurityOptions(object):
    """
    Kafka security option for configuring authentication
    """

    def __init__(self, ssl_certificates: str, username: str, password: str, sasl_mechanism: SaslMechanism = SaslMechanism.ScramSha256):
        """
            Create a new instance of SecurityOptions that is configured for SSL encryption with SASL authentication


            Parameters:

            ssl_certificates (string): The folder/file that contains the certificate authority certificate(s) to validate the ssl connection. Example: "./certificates/ca.cert"

            username (string): The username for the SASL authentication

            password (string): The password for the SASL authentication

            sasl_mechanism (SaslMechanism): The SASL mechanism to use. Defaulting to ScramSha256 for backward compatibility
        """

        sasl = SaslMechanismInterop.ScramSha256
        if sasl_mechanism is not None:
            sasl = ec.enum_to_another(sasl_mechanism, SaslMechanismInterop)
        self._interop = soi(soi.Constructor2(ssl_certificates, username, password, sasl))

    def get_net_pointer(self):
        return self._interop.get_interop_ptr__()
