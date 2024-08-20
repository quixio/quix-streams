from typing import Callable, Optional

from pydantic import SecretStr
from confluent_kafka.schema_registry import topic_subject_name_strategy

from quixstreams.utils.settings import BaseSettings
from quixstreams.models.serializers import SerializationContext

__all__ = [
    "SchemaRegistryClientConfig",
    "SchemaRegistrySerializationConfig",
]

SubjectNameStrategy = Callable[[SerializationContext, str], str]


class SchemaRegistryClientConfig(BaseSettings):
    """
    Configuration required to establish the connection with a Schema Registry.

    :param url: Schema Registry URL.
    :param ssl_ca_location: Path to CA certificate file used to verify the
        Schema Registry's private key.
    :param ssl_key_location: Path to the client's private key (PEM) used for
        authentication.
        >***NOTE:*** `ssl_certificate_location` must also be set.
    :param ssl_certificate_location: Path to the client's public key (PEM) used
        for authentication.
        >***NOTE:*** May be set without `ssl_key_location` if the private key is
        stored within the PEM as well.
    :param basic_auth_user_info: Client HTTP credentials in the form of
        `username:password`.
        >***NOTE:*** By default, userinfo is extracted from the URL if present.
    """

    url: str
    ssl_ca_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    basic_auth_user_info: Optional[SecretStr] = None


class SchemaRegistrySerializationConfig(BaseSettings):
    """
    Configuration that instructs Serializer how to handle communication with a
    Schema Registry.

    :param auto_register_schemas: If True, automatically register the configured schema
        with Confluent Schema Registry if it has not previously been associated with the
        relevant subject (determined via subject.name.strategy). Defaults to True.
    :param normalize_schemas: Whether to normalize schemas, which will transform schemas
        to have a consistent format, including ordering properties and references.
    :param use_latest_version: Whether to use the latest subject version for serialization.
        >***NOTE:*** There is no check that the latest schema is backwards compatible with the
        object being serialized. Defaults to False.
    :param subject_name_strategy: Callable(SerializationContext, str) -> str
        Defines how Schema Registry subject names are constructed. Standard naming
        strategies are defined in the confluent_kafka.schema_registry namespace.
        Defaults to topic_subject_name_strategy.
    """

    auto_register_schemas: bool = True
    normalize_schemas: bool = False
    use_latest_version: bool = False
    subject_name_strategy: SubjectNameStrategy = topic_subject_name_strategy
