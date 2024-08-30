import os
from typing import Optional

__all__ = ("QuixEnvironment", "QUIX_ENVIRONMENT")


class QuixEnvironment:
    """
    Class to access various Quix platform environment settings
    """

    SDK_TOKEN = "Quix__Sdk__Token"  # noqa: S105
    PORTAL_API = "Quix__Portal__Api"
    WORKSPACE_ID = "Quix__Workspace__Id"
    DEPLOYMENT_ID = "Quix__Deployment__Id"
    STATE_MANAGEMENT_ENABLED = "Quix__Deployment__State__Enabled"

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
    def portal_api(self) -> Optional[str]:
        """
        Return Quix Portal API url if set

        :return: portal API URL or None
        """
        return os.environ.get(self.PORTAL_API)

    @property
    def state_dir(self) -> str:
        """
        Return application state directory on Quix.
        :return: path to state dir
        """
        # TODO: Use env variables instead when they're available
        return "/app/state"


QUIX_ENVIRONMENT = QuixEnvironment()
