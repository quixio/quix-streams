from typing import Optional

from pydantic import SecretStr

from quixstreams.utils.settings import BaseSettings

__all__ = ["SchemaRegistryConfig"]


class SchemaRegistryConfig(BaseSettings):
    # TODO: docstrings
    url: str
    ssl_ca_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    basic_auth_user_info: Optional[SecretStr] = None
