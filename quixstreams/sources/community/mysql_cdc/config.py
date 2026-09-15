"""
Constructor-time configuration for the MySQL CDC source.

Two things live here, both of which have to be usable before anything has connected to
MySQL: the error type the connector raises for configuration problems, and the
transport-security settings that every connection it opens has to carry. Keeping them
out of `mysql_helper` is what lets `mysql_cdc.__init__` reject a contradictory
configuration without importing the MySQL drivers, and what keeps the constructor from
growing a wall of validation.

This module imports nothing else from the package, on purpose: `mysql_helper` imports
`TlsConfig` from here, so anything imported the other way would be a cycle.
"""

import ssl
from dataclasses import dataclass
from typing import Any, Dict, Optional

__all__ = ("MySqlCdcError", "TlsConfig", "require_positive")


class MySqlCdcError(Exception):
    """Raised for MySQL configuration/validation problems the user must fix."""


def require_positive(name: str, value: float) -> None:
    """Raise `MySqlCdcError` unless `value` is strictly positive."""
    if value <= 0:
        raise MySqlCdcError(f"{name} must be greater than 0, got {value}")


@dataclass(frozen=True)
class TlsConfig:
    """
    How the connector's connections to MySQL are secured.

    TLS is on by default and verification is off by default, which is deliberate and
    needs saying: MySQL 5.7.6+ and 8.x auto-generate a self-signed server certificate at
    first start, so requiring encryption connects out of the box while requiring
    verification would not. Giving `ca` is the one switch that turns verification on,
    because a CA bundle is the only thing that makes verification mean anything.

    An `ssl.SSLContext` is handed to pymysql rather than the `ssl_*` scalars because a
    truthy `ssl` argument is what selects pymysql's REQUIRED mode
    (`connections.py:291-297`). With no ssl arguments at all, pymysql 1.0-1.2 negotiate
    PREFERRED mode and fall back to plaintext without telling anyone
    (`connections.py:298-303,925-931`) - which is what the connector used to do.
    """

    enabled: bool = True
    ca: Optional[str] = None
    cert: Optional[str] = None
    key: Optional[str] = None
    verify_cert: Optional[bool] = None
    verify_identity: bool = False

    @property
    def verifies(self) -> bool:
        """True when the server certificate is checked against a CA."""
        if self.verify_cert is not None:
            return self.verify_cert
        return self.ca is not None

    def validate(self) -> None:
        """
        Reject every combination that cannot mean what it says.

        Each of these has a silent reading ("verify against nothing", "send a key with
        no certificate", "disable TLS but here is a CA") and a loud one. The loud one is
        the only safe answer for a security setting.
        """
        if not self.enabled:
            conflicting = [
                name
                for name, value in (
                    ("tls_ca", self.ca),
                    ("tls_cert", self.cert),
                    ("tls_key", self.key),
                    ("tls_verify_cert", self.verify_cert),
                    ("tls_verify_identity", self.verify_identity or None),
                )
                if value
            ]
            if conflicting:
                raise MySqlCdcError(
                    f"tls_enabled=False cannot be combined with {', '.join(conflicting)}: "
                    "there is no connection to secure. Drop the other tls_* parameters, "
                    "or set tls_enabled=True."
                )
            return

        if self.verify_cert and self.ca is None:
            raise MySqlCdcError(
                "tls_verify_cert=True needs a CA to verify against: set tls_ca to the "
                "PEM bundle containing the CA that signed the MySQL server certificate."
            )
        if self.verify_identity and not self.verifies:
            raise MySqlCdcError(
                "tls_verify_identity=True checks the hostname on a certificate that is "
                "not being verified. Set tls_ca (and leave tls_verify_cert unset or "
                "True) so the certificate itself is checked first."
            )
        if self.key and not self.cert:
            raise MySqlCdcError(
                "tls_key was given without tls_cert. A client key is only usable "
                "alongside the client certificate it belongs to."
            )

    def connect_kwargs(self) -> Dict[str, Any]:
        """
        The pymysql connection arguments this configuration implies.

        A fresh dict every call: `BinLogStreamReader` mutates the
        `connection_settings` dict it is handed (`binlogstream.py:241`), so a shared one
        would leak the library's edits into the next connection.
        """
        if not self.enabled:
            # Explicit, rather than "pass no ssl argument": with no argument pymysql
            # still attempts TLS in PREFERRED mode, so "disabled" would not be.
            return {"ssl_disabled": True}
        return {"ssl": self._context()}

    def _context(self) -> ssl.SSLContext:
        """
        Build the SSL context, in an order `ssl` accepts.

        `create_default_context()` returns `check_hostname=True` with
        `CERT_REQUIRED`, and assigning `CERT_NONE` while `check_hostname` is still True
        raises `ValueError` - so hostname checking is switched off first and back on
        afterwards if it was asked for.
        """
        context = ssl.create_default_context(cafile=self.ca)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED if self.verifies else ssl.CERT_NONE
        if self.verify_identity:
            context.check_hostname = True
        if self.cert:
            context.load_cert_chain(self.cert, keyfile=self.key)
        return context

    def describe(self) -> str:
        """One line for the start-up log, so a deployment can see what it got."""
        if not self.enabled:
            return "TLS: disabled (tls_enabled=False) - the connection is plaintext"
        if not self.verifies:
            return (
                "TLS: required, server certificate NOT verified (no tls_ca). The "
                "connection is encrypted but the server is not authenticated."
            )
        identity = "and hostname" if self.verify_identity else "hostname NOT checked"
        return (
            f"TLS: required, server certificate verified against {self.ca} ({identity})"
        )
