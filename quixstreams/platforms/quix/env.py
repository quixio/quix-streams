import os
from typing import Optional

__all__ = ("QuixEnvironment", "QUIX_ENVIRONMENT")


class QuixEnvironment:
    """
    A class to access various Quix Streams environment variables
    """

    SDK_TOKEN = "Quix__Sdk__Token"  # noqa: S105
    BROKER_ADDRESS = "Quix__Broker__Address"
    PORTAL_API = "Quix__Portal__Api"
    WORKSPACE_ID = "Quix__Workspace__Id"
    DEPLOYMENT_ID = "Quix__Deployment__Id"
    STATE_MANAGEMENT_ENABLED = "Quix__Deployment__State__Enabled"
    STATE_PATH = "Quix__Deployment__State__Path"  # Set by Quix platform
    STATE_DIR = "Quix__State__Dir"  # User override
    CONSUMER_GROUP = "Quix__Consumer_Group"

    @property
    def state_management_enabled(self) -> bool:
        """
        Check whether "State management" is enabled for the current deployment
        :return: True if state management is enabled, otherwise False
        """
        return os.environ.get(self.STATE_MANAGEMENT_ENABLED, "") == "true"

    @property
    def deployment_id(self) -> Optional[str]:
        """
        Return current Quix deployment id.

        This variable is meant to be set only by Quix Platform and only
        when the application is deployed.

        :return: deployment id or None
        """
        return os.environ.get(self.DEPLOYMENT_ID)

    @property
    def workspace_id(self) -> Optional[str]:
        """
        Return Quix workspace id if set
        :return: workspace id or None
        """
        return os.environ.get(self.WORKSPACE_ID)

    @property
    def state_dir(self) -> Optional[str]:
        """
        Return application state directory on Quix.
        Checks Quix__Deployment__State__Path first (set by platform),
        then falls back to Quix__State__Dir (deprecated user override).
        :return: path to state dir
        """
        legacy = os.environ.get(self.STATE_DIR)
        if legacy:
            import warnings

            warnings.warn(
                "Quix__State__Dir is deprecated, use state.path in quix.yaml instead",
                DeprecationWarning,
            )
        return os.environ.get(self.STATE_PATH) or legacy

    @property
    def portal_api(self) -> Optional[str]:
        """
        Quix Portal API URL
        """
        return os.environ.get(self.PORTAL_API)

    @property
    def broker_address(self) -> Optional[str]:
        """
        Kafka broker address
        """
        return os.environ.get(self.BROKER_ADDRESS)

    @property
    def sdk_token(self) -> Optional[str]:
        """
        Quix SDK token
        """
        return os.environ.get(self.SDK_TOKEN)

    @property
    def consumer_group(self) -> Optional[str]:
        """
        Kafka consumer group
        """
        return os.environ.get(self.CONSUMER_GROUP)


QUIX_ENVIRONMENT = QuixEnvironment()
