import logging

from .env import QUIX_ENVIRONMENT
from .exceptions import StateManagementDisabledError

logger = logging.getLogger(__name__)
__all__ = ("check_state_management_enabled", "check_state_dir")


def check_state_management_enabled():
    """
    Check if State Management feature is enabled for the current deployment on
    Quix platform.
    If it's disabled, the exception will be raised.

    """
    if QUIX_ENVIRONMENT.deployment_id and not QUIX_ENVIRONMENT.state_management_enabled:
        raise StateManagementDisabledError(
            f"State Management feature is disabled for Quix deployment "
            f'"{QUIX_ENVIRONMENT.deployment_id}". '
            f"Please enable it in the deployment settings to use state."
        )


def check_state_dir(state_dir: str):
    """
    Check if Application "state_dir" matches the state dir on Quix platform.

    If it doesn't match, the warning will be logged.

    :param state_dir: application state_dir path
    """
    if QUIX_ENVIRONMENT.deployment_id and state_dir != QUIX_ENVIRONMENT.state_dir:
        logger.warning(
            f'Path to state directory "{state_dir}" does not match the state directory'
            f' "{QUIX_ENVIRONMENT.state_dir}" on Quix Platform. '
            f"The state will not be shared between replicas "
            f"of this deployment, and it may be lost on restart."
        )
