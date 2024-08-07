from pydantic import AliasGenerator, SecretStr
from pydantic_settings import BaseSettings as _BaseSettings, SettingsConfigDict

__all__ = ["BaseSettings"]


class BaseSettings(_BaseSettings):

    model_config = SettingsConfigDict(
        alias_generator=AliasGenerator(
            # used during model_dumps
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        ),
    )

    def as_dict(self, plaintext_secrets: bool = False) -> dict:
        """
        Dump any non-empty config values as a dictionary.

        :param plaintext_secrets: whether secret values are plaintext or obscured (***)
        :return: a dictionary
        """
        dump = self.model_dump(by_alias=True, exclude_none=True)
        if plaintext_secrets:
            for field, value in dump.items():
                if isinstance(value, SecretStr):
                    dump[field] = dump[field].get_secret_value()
        return dump

    def __str__(self) -> str:
        return str(self.as_dict())
