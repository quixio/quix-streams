from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AliasGenerator, SecretStr, AliasChoices, Field
from pydantic.functional_validators import BeforeValidator

from typing import Literal, Optional, Callable, Tuple
from typing_extensions import Self, Annotated


__all__ = ("ConnectionConfig",)

_aliases = {"sasl_mechanism": AliasChoices("sasl_mechanism", "sasl_mechanisms")}


class ConnectionConfig(BaseSettings):

    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            # used during model_dump
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        ),
    )

    @classmethod
    def from_librdkafka_dict(cls, d: dict, ignore_extras: bool = False) -> Self:
        d = {name.replace(".", "_"): v for name, v in d.items()}
        if ignore_extras:
            keys = set(cls.model_fields.keys()) | {
                [a for a in v.choices] for v in _aliases.values()
            }
            d = {k: v for k, v in d.items() if k in keys}
        return cls(**d)

    def as_librdkafka_dict(self) -> dict:
        dump = self.model_dump(by_alias=True, exclude_none=True)
        dump["sasl.password"] = dump["sasl.password"].get_secret_value()
        return dump

    def as_printable_json(self, indent=2) -> str:
        return self.model_dump_json(by_alias=True, exclude_none=True, indent=indent)

    def __str__(self) -> str:
        return self.as_printable_json()

    bootstrap_servers: str
    security_protocol: Annotated[
        Optional[Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None

    # ----------------- SASL SETTINGS -----------------
    # What sasl_mechanism is doing:
    # 1. Automatically upper() the value since it's case sensitive
    # 2. Allows "sasl_mechanisms" (or "." if librdkafka) and sets it to sasl_mechanism
    sasl_mechanism: Annotated[
        Optional[
            Literal["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"]
        ],
        Field(validation_alias=_aliases["sasl_mechanism"]),
        BeforeValidator(lambda v: v.upper() if v is not None else v),
    ] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
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
    sasl_oauthbearer_method: Annotated[
        Optional[Literal["default", "oidc"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None
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
    ssl_endpoint_identification_algorithm: Annotated[
        Optional[Literal["none", "https"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None
