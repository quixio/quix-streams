from typing import Literal, Optional, Callable, Tuple, List, get_args

from pydantic import AliasGenerator, SecretStr, AliasChoices, Field
from pydantic.functional_validators import BeforeValidator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self, Annotated

__all__ = ("ConnectionConfig",)

MECHANISM_ALIAS = AliasChoices("sasl_mechanism", "sasl_mechanisms")


class ConnectionConfig(BaseSettings):
    """
    Provides an interface for all librdkafka connection-based configs.

    Allows converting to or from a librdkafka dictionary.

    Also obscures secrets and handles any case sensitivity issues.
    """

    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            # used during model_dumps
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        ),
    )

    @classmethod
    def secret_fields(cls) -> List[str]:
        """
        Get all the fields that are of the type "SecretStr" (passwords)

        :return: a list of secret field names
        """
        fields = []
        for name, info in ConnectionConfig.model_fields.items():
            if SecretStr in get_args(info.annotation):
                fields.append(name)
        return fields

    @classmethod
    def from_librdkafka_dict(cls, config: dict, ignore_extras: bool = False) -> Self:
        """
        Create a `ConnectionConfig` from a librdkafka config dictionary.

        :param config: a dict of configs (like {"bootstrap.servers": "url"})
        :param ignore_extras: Ignore non-connection settings (else raise exception)

        :return: a ConnectionConfig
        """
        config = {name.replace(".", "_"): v for name, v in config.items()}
        if ignore_extras:
            valid_keys = set(cls.model_fields.keys()) | set(MECHANISM_ALIAS.choices)
            config = {k: v for k, v in config.items() if k in valid_keys}
        return cls(**config)

    def as_librdkafka_dict(self) -> dict:
        """
        Dump any non-empty config values as a librdkafka dictionary.

        >***NOTE***: Dumps all secret fields in plaintext.

        :return: a librdkafka-compatible dictionary
        """
        dump = self.model_dump(by_alias=True, exclude_none=True)
        for field in self.secret_fields():
            field = field.replace("_", ".")
            if dump.get(field):
                dump[field] = dump[field].get_secret_value()
        return dump

    def as_printable_json(self, indent: int = 2) -> str:
        """
        Dump any non-empty config values as a librdkafka-formatted JSON string.

        Safely obscures the any "secret" fields (passwords).

        :param indent: the indent of the JSON

        :return: a string-dumped JSON
        """
        return self.model_dump_json(by_alias=True, exclude_none=True, indent=indent)

    def __str__(self) -> str:
        return self.as_printable_json()

    bootstrap_servers: str
    security_protocol: Annotated[
        Optional[Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None

    # ----------------- SASL SETTINGS -----------------
    # Allows "sasl_mechanisms" (or "." if librdkafka) and sets it to sasl_mechanism
    sasl_mechanism: Annotated[
        Optional[
            Literal["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"]
        ],
        Field(validation_alias=MECHANISM_ALIAS),
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
    sasl_oauthbearer_client_secret: Optional[SecretStr] = None
    sasl_oauthbearer_scope: Optional[str] = None
    sasl_oauthbearer_extensions: Optional[str] = None
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None

    # ----------------- SSL SETTINGS -----------------
    ssl_cipher_suites: Optional[str] = None
    ssl_curves_list: Optional[str] = None
    ssl_sigalgs_list: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None
    ssl_key_pem: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_certificate_pem: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_ca_pem: Optional[str] = None
    ssl_ca_certificate_stores: Optional[str] = None
    ssl_crl_location: Optional[str] = None
    ssl_keystore_location: Optional[str] = None
    ssl_keystore_password: Optional[SecretStr] = None
    ssl_providers: Optional[str] = None
    ssl_engine_location: Optional[str] = None
    ssl_engine_id: Optional[str] = None
    enable_ssl_certificate_verification: Optional[bool] = None
    ssl_endpoint_identification_algorithm: Annotated[
        Optional[Literal["none", "https"]],
        BeforeValidator(lambda v: v.lower() if v is not None else v),
    ] = None
