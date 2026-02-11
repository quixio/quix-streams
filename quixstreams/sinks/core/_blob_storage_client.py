"""
Blob Storage Client

Unified blob storage client that abstracts away provider-specific implementations.
Uses quixportal for flexible blob storage access (Azure, AWS S3, GCP, MinIO, local).
"""

import concurrent.futures
import logging
from typing import Any, Callable, Dict, List, Optional

try:
    from quixportal import get_filesystem
    from quixportal.storage.config import BlobStorageProvider, load_config_from_env
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[quixdatalake] '
        '--extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/" '
        "to use QuixLakeBlobStorageSink"
    ) from exc

logger = logging.getLogger(__name__)


def get_bucket_name() -> str:
    """
    Extract the bucket/container name from the quixportal configuration.

    This reads the Quix__BlobStorage__Connection__Json environment variable
    and extracts the bucket name based on the provider type.

    :returns: The bucket name for S3/MinIO or container name for Azure
    :raises ValueError: If configuration is missing or bucket name cannot be determined
    """
    config = load_config_from_env()

    if config.provider in (
        BlobStorageProvider.S3,
        BlobStorageProvider.MINIO,
        BlobStorageProvider.S3_COMPATIBLE,
        BlobStorageProvider.GCP,
    ):
        if config.s3_compatible:
            return config.s3_compatible.bucket_name
        raise ValueError(f"S3-compatible config missing for provider {config.provider}")

    elif config.provider == BlobStorageProvider.AZURE:
        if config.azure_blob_storage:
            return config.azure_blob_storage.container_name
        raise ValueError("Azure blob storage config missing")

    elif config.provider == BlobStorageProvider.LOCAL:
        # For local storage, use the directory name as "bucket"
        if config.local_storage:
            import os

            return os.path.basename(config.local_storage.directory_path.rstrip("/\\"))
        raise ValueError("Local storage config missing")

    raise ValueError(f"Unsupported provider: {config.provider}")


class BlobStorageClient:
    """
    Unified blob storage client that abstracts away provider-specific implementations.
    Uses quixportal for flexible blob storage access.
    """

    def __init__(self, base_path: str = "", max_workers: int = 10):
        """
        Initialize blob storage client via quixportal.

        :param base_path: Optional base path prefix for all operations.
            If empty, filesystem is used without DirFileSystem wrapper.
        :param max_workers: Maximum number of concurrent upload workers.
        """
        # Only pass base_path if non-empty to avoid quixportal wrapping with DirFileSystem(base_path=".")
        # which causes "Access denied" errors on GCP S3-compatible storage
        if base_path:
            self._fs = get_filesystem(base_path=base_path)
        else:
            self._fs = get_filesystem()
        self.base_path = base_path
        self._max_workers = max_workers
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

    def _get_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Get or create thread pool executor for concurrent uploads."""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            )
        return self._executor

    def list_objects(self, prefix: str, max_keys: int = 100) -> List[Dict[str, Any]]:
        """
        List objects in blob storage with a given prefix.

        :param prefix: Object prefix/path
        :param max_keys: Maximum number of objects to return
        :returns: List of dicts with 'Key' and 'Size' keys
        """
        try:
            result: List[Dict[str, Any]] = []
            found_files: set[str] = set()

            # Build base pattern
            base_pattern = prefix if prefix else ""
            if base_pattern and not base_pattern.endswith("/"):
                base_pattern = base_pattern + "/"

            # Try multiple depth levels (up to 10 levels deep)
            for depth in range(0, 10):
                if len(result) >= max_keys:
                    break

                if depth == 0:
                    pattern = f"{base_pattern}*" if base_pattern else "*"
                else:
                    pattern = f"{base_pattern}{'*/' * depth}*"

                try:
                    files = self._fs.glob(pattern)
                    for f in files:
                        if f in found_files:
                            continue
                        if len(result) >= max_keys:
                            break
                        try:
                            if self._fs.exists(f):
                                size = self._fs.size(f)
                                result.append(
                                    {"Key": f, "Size": size if size is not None else 0}
                                )
                                found_files.add(f)
                        except Exception as e:
                            logger.debug(f"Error processing {f}: {e}")
                except Exception as e:
                    logger.debug(f"Glob pattern at depth {depth} failed: {e}")
                    continue

            return result
        except Exception as e:
            logger.warning(f"Error listing objects with prefix {prefix}: {e}")
            return []

    def put_object(self, key: str, body: bytes) -> None:
        """
        Put/upload data to blob storage.

        :param key: Object key/path
        :param body: Object data as bytes
        """
        try:
            with self._fs.open(key, "wb") as f:
                f.write(body)
            logger.debug(f"Uploaded {key}")
        except Exception as e:
            logger.error(f"Error uploading {key}: {e}")
            raise

    def put_object_async(
        self, key: str, body: bytes, callback: Optional[Callable] = None
    ) -> concurrent.futures.Future:
        """
        Asynchronously put/upload data to blob storage.

        :param key: Object key/path
        :param body: Object data as bytes
        :param callback: Optional callback function to call on completion
        :returns: Future object that can be used to wait for completion
        """

        def _upload():
            self.put_object(key, body)
            if callback:
                callback(key)
            return key

        future = self._get_executor().submit(_upload)
        return future

    def head_object(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata about an object without reading its contents.

        :param key: Object key
        :returns: Dict with 'Key' and 'Size' keys, or None if object doesn't exist
        """
        try:
            if self._fs.exists(key):
                size = self._fs.size(key)
                return {"Key": key, "Size": size if size is not None else 0}
            return None
        except Exception as e:
            logger.warning(f"Error getting metadata for {key}: {e}")
            return None

    def exists(self, key: str) -> bool:
        """
        Check if an object exists in blob storage.

        :param key: Object key
        :returns: True if object exists, False otherwise
        """
        try:
            return self._fs.exists(key)
        except Exception as e:
            logger.warning(f"Error checking existence of {key}: {e}")
            return False

    def delete_object(self, key: str) -> None:
        """
        Delete a single object from blob storage.

        :param key: Object key
        """
        try:
            if self._fs.exists(key):
                try:
                    self._fs.rm_file(key)
                except (NotImplementedError, AttributeError):
                    self._fs.rm(key)
                logger.debug(f"Deleted {key}")
        except Exception as e:
            logger.warning(f"Failed to delete {key}: {e}")

    def delete_objects(self, keys: List[str]) -> None:
        """
        Delete multiple objects from blob storage.

        :param keys: List of object keys to delete
        """
        for key in keys:
            self.delete_object(key)

    def get_object(self, key: str) -> bytes:
        """
        Get/download data from blob storage.

        :param key: Object key
        :returns: Object data as bytes
        """
        try:
            with self._fs.open(key, "rb") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error downloading {key}: {e}")
            raise

    def ensure_path_exists(self, auto_create: bool = True) -> bool:
        """
        Ensure the base path/bucket is accessible, optionally creating it.

        :param auto_create: If True, attempt to create the path if it doesn't exist
        :returns: True if path exists or was created successfully, False otherwise
        """
        try:
            # Try to list the root to verify access
            self._fs.ls("")
            logger.info("Successfully connected to blob storage")
            return True
        except Exception as e:
            if auto_create:
                try:
                    # Try to create the path/bucket
                    self._fs.mkdir("")
                    logger.info("Created blob storage path")
                    return True
                except Exception as create_error:
                    logger.warning(
                        f"Could not create blob storage path: {create_error}. "
                        "Continuing anyway - path may already exist or be created on first write."
                    )
                    return True  # Continue anyway with fallback behavior
            else:
                logger.error(f"Failed to access blob storage: {e}")
                return False

    def shutdown(self):
        """Shutdown the thread pool executor."""
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
