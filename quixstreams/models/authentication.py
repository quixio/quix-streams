from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AliasChoices, ConfigDict, AliasGenerator
import json

from typing import Literal, Optional, Callable, Tuple


# notes:
# multiple aliases: Field(validation_alias=AliasChoices("sasl_mechanism", "sasl_mechanisms", "mechanisms", "mechanism"), default=None)

__all__ = ("SASLConfig", "SSLConfig")


class SASLConfig(BaseSettings):
    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        )
    )

    @classmethod
    def from_confluent_dict(cls, d, ignore_extras=False):
        if ignore_extras:
            allowed = list(cls.model_fields.keys())
            return cls(
                **{
                    name.replace(".", "_"): v
                    for name, v in d.items()
                    if name in allowed
                }
            )
        return cls(**{name.replace(".", "_"): v for name, v in d.items()})

    @classmethod
    def from_file(cls, filepath: str, ignore_extras=False):
        if filepath.endswith("json"):
            with open(filepath, "r") as f:
                settings = json.load(f)
            return cls.from_confluent_dict(settings, ignore_extras=ignore_extras)
        if filepath.endswith("env"):
            return cls(_env_file=filepath)
        raise ValueError("only '.env' and '.json' file types supported")

    def as_confluent_dict(self):
        return self.model_dump(by_alias=True, exclude_none=True)

    security_protocol: Optional[
        Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]
    ] = None
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

    # see https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-configuration
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


class SSLConfig(BaseSettings):
    security_protocol: Optional[
        Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]
    ]
    ssl_cipher_suites: Optional[str]
    ssl_curves_list: Optional[str]
    ssl_sigalgs_list: Optional[str]
    ssl_key_location: Optional[str]
    ssl_key_password: Optional[str]
    ssl_key_pem: Optional[str]
    ssl_certificate_location: Optional[str]
    ssl_certificate_pem: Optional[str]
    ssl_ca_location: Optional[str]
    ssl_ca_pem: Optional[str]
    ssl_ca_certificate_stores: Optional[str]
    ssl_crl_location: Optional[str]
    ssl_keystore_location: Optional[str]
    ssl_keystore_password: Optional[str]
    ssl_providers: Optional[str]
    ssl_engine_location: Optional[str]
    ssl_engine_id: Optional[str]
    enable_ssl_certificate_verification: Optional[bool]
    ssl_endpoint_identification_algorithm: Optional[Literal["none", "https"]]

    # not sure how/if these are used (seem to be more for librdkafka)
    # ssl_key
    # ssl_certificate
    # ssl_ca
    # ssl.certificate.verify_cb
