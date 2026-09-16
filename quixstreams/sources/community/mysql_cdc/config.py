"""Configuration the source builds before it imports or contacts anything."""

import ssl
from dataclasses import dataclass
from typing import Any, Dict, Optional

__all__ = ("ConnectionTimeouts", "MySqlCdcError", "TlsConfig", "require_positive")

_MIN_CONNECT_TIMEOUT = 10.0
_MIN_READ_TIMEOUT = 30.0


class MySqlCdcError(Exception):
    """Raised for MySQL configuration/validation problems the user must fix."""


def require_positive(name: str, value: float) -> None:
    """
    :raises MySqlCdcError: unless `value` is strictly positive.
    """
    if value <= 0:
        raise MySqlCdcError(f"{name} must be greater than 0, got {value}")


@dataclass(frozen=True)
class TlsConfig:
    """
    How the connector's connections to MySQL are secured.

    :param enabled: require an encrypted connection.
    :param ca: path to a PEM CA bundle; giving one turns verification on.
    :raises MySqlCdcError: if a CA is given with encryption disabled.
    """

    enabled: bool = True
    ca: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.enabled and self.ca is not None:
            raise MySqlCdcError(
                "tls_enabled=False cannot be combined with tls_ca: there is no "
                "connection to secure. Drop tls_ca, or set tls_enabled=True."
            )

    @property
    def verifies(self) -> bool:
        """True when the server certificate is checked against a CA."""
        return self.ca is not None

    def connect_kwargs(self) -> Dict[str, Any]:
        """
        The pymysql connection arguments this configuration implies.

        A fresh dict every call: `BinLogStreamReader.__init__` keeps the dict it is
        handed and setdefault()s "charset" into it.
        """
        if not self.enabled:
            # Passing no ssl argument at all is not the same thing: pymysql then takes
            # its PREFERRED branch, which tries TLS and accepts plaintext.
            return {"ssl_disabled": True}
        return {"ssl": self._context()}

    def _context(self) -> ssl.SSLContext:
        context = ssl.create_default_context(cafile=self.ca)
        # Cleared because `pymysql._create_ssl_ctx` clears it on the context it builds
        # itself, and MySQL's self-generated certificates do not pass it.
        context.verify_flags &= ~ssl.VERIFY_X509_STRICT
        # CERT_NONE cannot be assigned while check_hostname is True.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED if self.verifies else ssl.CERT_NONE
        context.check_hostname = self.verifies
        return context

    def describe(self) -> str:
        """One line for the start-up log."""
        if not self.enabled:
            return "TLS: disabled (tls_enabled=False) - the connection is plaintext"
        if not self.verifies:
            return (
                "TLS: required, server certificate NOT verified (no tls_ca). The "
                "connection is encrypted but the server is not authenticated."
            )
        return (
            f"TLS: required, server certificate and hostname verified against {self.ca}"
        )


@dataclass(frozen=True)
class ConnectionTimeouts:
    """Socket timeouts, in seconds, for every connection the connector opens."""

    connect: float
    read: float
    write: float

    @classmethod
    def derive(
        cls,
        commit_interval: float,
        retry_backoff_secs: float,
        shutdown_timeout: float,
    ) -> "ConnectionTimeouts":
        """
        Size the timeouts from the cadences the source already runs on.

        :param commit_interval: the source's commit cadence.
        :param retry_backoff_secs: the source's maximum reconnect backoff.
        :param shutdown_timeout: the source's graceful-shutdown budget.
        """
        read = max(_MIN_READ_TIMEOUT, commit_interval * 4, shutdown_timeout * 2)
        return cls(
            connect=max(_MIN_CONNECT_TIMEOUT, retry_backoff_secs),
            read=read,
            write=read,
        )

    def connect_kwargs(self) -> Dict[str, Any]:
        """The pymysql connection arguments these timeouts imply."""
        return {
            "connect_timeout": self.connect,
            "read_timeout": self.read,
            "write_timeout": self.write,
        }
