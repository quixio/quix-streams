from .saslmechanism import SaslMechanism
from ..helpers.enumconverter import EnumConverter as ec
from ..native.Python.QuixStreamsStreaming.Configuration.SaslMechanism import SaslMechanism as SaslMechanismInterop
from ..native.Python.QuixStreamsStreaming.Configuration.SecurityOptions import SecurityOptions as soi


class SecurityOptions(object):
    """
    A class representing security options for configuring SSL encryption with SASL authentication in Kafka.
    """

    def __init__(self, ssl_certificates: str, username: str, password: str, sasl_mechanism: SaslMechanism = SaslMechanism.ScramSha256):
        """
        Initializes a new instance of SecurityOptions configured for SSL encryption with SASL authentication.

        Args:
            ssl_certificates: The path to the folder or file containing the certificate authority
                certificate(s) used to validate the SSL connection.
                Example: "./certificates/ca.cert"
            username: The username for SASL authentication.
            password: The password for SASL authentication.
            sasl_mechanism: The SASL mechanism to use. Defaults to ScramSha256.
        """

        sasl = SaslMechanismInterop.ScramSha256
        if sasl_mechanism is not None:
            sasl = ec.enum_to_another(sasl_mechanism, SaslMechanismInterop)
        self._interop = soi(soi.Constructor2(ssl_certificates, username, password, sasl))

    def get_net_pointer(self):
        """
        Retrieves the .NET pointer for the current SecurityOptions instance.

        Returns:
            ctypes.c_void_p: The .NET pointer.
        """
        return self._interop.get_interop_ptr__()
