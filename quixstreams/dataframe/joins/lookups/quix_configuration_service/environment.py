import os

# Base delay (in seconds) for exponential backoff when retrying configuration version fetches.
VERSION_RETRY_BASE_DELAY = int(os.getenv("ENRICH_VERSION_RETRY_BASE_DELAY", 1))

# Maximum delay (in seconds) for exponential backoff when retrying configuration version fetches.
VERSION_RETRY_MAX_DELAY = int(
    os.getenv("ENRICH_VERSION_RETRY_MAX_DELAY", 600)
)  # seconds (10 minutes)

# Name of the Quix deployment replica, used to distinguish consumer groups in multi-replica deployments.
QUIX_REPLICA_NAME = os.getenv("Quix__Deployment__ReplicaName")
