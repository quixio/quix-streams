from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AliasGenerator, Field

from typing import Literal, Optional, Callable, Tuple
from typing_extensions import Self

# notes:
# multiple aliases: Field(validation_alias=AliasChoices("sasl_mechanism", "sasl_mechanisms", "mechanisms", "mechanism"), default=None)

__all__ = ("ConnectionConfig",)


class ConnectionConfig(BaseSettings):
    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            # used during model_dump
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        )
    )

    @classmethod
    def from_confluent_dict(cls, d: dict, ignore_extras: bool = False) -> Self:
        d = {name.replace(".", "_").lower(): v for name, v in d.items()}
        if ignore_extras:
            allowed = list(cls.model_fields.keys())
            return cls(**{name: v for name, v in d.items() if name in allowed})
        return cls(**d)

    def as_confluent_dict(self) -> dict:
        return self.model_dump(by_alias=True, exclude_none=True)

    bootstrap_servers: str = Field()
    security_protocol: Optional[
        Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]
    ] = None

    # ----------------- SASL SETTINGS -----------------
    sasl_mechanism: Optional[
        Literal["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"]
    ] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    sasl_kerberos_kinit_cmd: Optional[str] = None
    sasl_kerberos_keytab: Optional[str] = None
    sasl_kerberos_min_time_before_relogin: Optional[int] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_principal: Optional[str] = None
    # for oauth_cb, see https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-configuration
    oauth_cb: Optional[Callable[[str], Tuple[str, float]]] = None
    sasl_oauthbearer_config: Optional[str] = None
    enable_sasl_oauthbearer_unsecure_jwt: Optional[bool] = None
    oauthbearer_token_refresh_cb: Optional[Callable] = None
    sasl_oauthbearer_method: Optional[Literal["default", "oidc"]] = None
    sasl_oauthbearer_client_id: Optional[str] = None
    sasl_oauthbearer_client_secret: Optional[str] = None
    sasl_oauthbearer_scope: Optional[str] = None
    sasl_oauthbearer_extensions: Optional[str] = None
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None

    # ----------------- SSL SETTINGS -----------------
    ssl_cipher_suites: Optional[str] = None
    ssl_curves_list: Optional[str] = None
    ssl_sigalgs_list: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[str] = None
    ssl_key_pem: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_certificate_pem: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_ca_pem: Optional[str] = None
    ssl_ca_certificate_stores: Optional[str] = None
    ssl_crl_location: Optional[str] = None
    ssl_keystore_location: Optional[str] = None
    ssl_keystore_password: Optional[str] = None
    ssl_providers: Optional[str] = None
    ssl_engine_location: Optional[str] = None
    ssl_engine_id: Optional[str] = None
    enable_ssl_certificate_verification: Optional[bool] = None
    ssl_endpoint_identification_algorithm: Optional[Literal["none", "https"]] = None
