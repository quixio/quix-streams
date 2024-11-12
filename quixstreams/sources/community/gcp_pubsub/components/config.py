import logging
import os
from dataclasses import dataclass
from typing import Optional

__all__ = ("GCPPubSubConfig",)


logger = logging.getLogger(__name__)


@dataclass
class GCPPubSubConfig:
    """
    A convenient way to utilize authentication that normally can only be done
    with environment variables.

    You should use either one or the other of these settings.

    :param credentials_path: A path to your Google Application credentials JSON file
    :param emulated_host_url: the address of an emulated host
    """

    credentials_path: Optional[str] = None
    emulated_host_url: Optional[str] = None

    def __post_init__(self):
        if emulated_host_env := os.getenv("PUBSUB_EMULATOR_HOST"):
            if self.emulated_host_url and self.emulated_host_url != emulated_host_env:
                raise ValueError(
                    f"'emulated_host_url' ('{self.emulated_host_url}') and "
                    "environment variable 'PUBSUB_EMULATOR_HOST' "
                    f"('{emulated_host_env}') are both used; set one only."
                )
            print("USING EMULATOR HOST!")
            return
        if self.emulated_host_url:
            logger.info(
                "Setting environment variable 'PUBSUB_EMULATOR_HOST' "
                "to the provided 'emulated_host_url'"
            )
            os.environ["PUBSUB_EMULATOR_HOST"] = self.emulated_host_url
            return

        if creds_env := os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            if self.credentials_path and self.credentials_path != creds_env:
                raise ValueError(
                    f"'credentials_path' ('{self.emulated_host_url}') and "
                    "environment variable 'GOOGLE_APPLICATION_CREDENTIALS' "
                    f"('{creds_env}') are both used; set one only."
                )
            return
        if self.credentials_path:
            logger.info(
                "Setting environment variable 'GOOGLE_APPLICATION_CREDENTIALS' "
                "to the provided 'credentials_path'"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
            return
        raise ValueError("Must provide a 'credentials_path' or 'emulated_host_url'")
