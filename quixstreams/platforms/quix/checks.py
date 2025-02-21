import logging
import warnings
from pathlib import Path

from .env import QUIX_ENVIRONMENT

logger = logging.getLogger(__name__)
__all__ = ("check_state_management_enabled", "check_state_dir", "is_quix_deployment")


def is_quix_deployment() -> bool:
    """
    Check if the current deployment is a Quix deployment.
    """
    return bool(QUIX_ENVIRONMENT.deployment_id)


def check_state_management_enabled() -> None:
    """
    Check if State Management feature is enabled for the current deployment on
    Quix platform.

    If it's disabled, the warning will be logged.

    """
    if is_quix_deployment() and not QUIX_ENVIRONMENT.state_management_enabled:
        warnings.warn(
            f"State Management feature is disabled for Quix deployment "
            f'"{QUIX_ENVIRONMENT.deployment_id}". '
            f"You may enable it in the deployment settings to share "
            f"the state between replicas.",
            category=RuntimeWarning,
        )


def check_state_dir(state_dir: Path) -> None:
    """
    Check if Application "state_dir" matches the state dir on Quix platform.

    If it doesn't match, the warning will be logged.

    :param state_dir: application state_dir path
    """

    state_dir_abs = str(state_dir.absolute())
    if QUIX_ENVIRONMENT.deployment_id and state_dir_abs != QUIX_ENVIRONMENT.state_dir:
        warnings.warn(
            f'Path to state directory "{state_dir_abs}" does not match '
            f'the state directory "{QUIX_ENVIRONMENT.state_dir}" on Quix Platform. '
            f"The state will not be shared between replicas "
            f"of this deployment, and it may be lost on restart.",
            category=RuntimeWarning,
        )
