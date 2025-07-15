import sys
import time
from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup import Lookup
from quixstreams.dataframe.joins.lookups.quix_configuration_service.models import (
    Configuration,
    ConfigurationVersion,
    Event,
    Field,
)


@pytest.fixture
def lookup():
    """Helper method to create a mock Lookup instance for testing."""
    # Create a mock topic
    mock_topic = Mock()
    mock_topic.name = "test-topic"

    # Patch the consumer and client to avoid actual Kafka/HTTP connections
    with (
        patch(
            "quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup.Consumer"
        ),
        patch(
            "quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup.httpx.Client"
        ),
        patch.object(Lookup, "_start"),
    ):
        lookup = Lookup(
            topic=mock_topic,
            broker_address="dummy:9092",
            consumer_poll_timeout=1.0,
        )
        # Clear any auto-started state
        lookup._configurations = {}
        yield lookup


def create_configuration_version(
    id: str = "test-config",
    version: int = 1,
    valid_from: float = 0,
    sha256sum: str = "test-hash",
    content_url: str = "http://example.com/config",
) -> ConfigurationVersion:
    """Helper method to create test configuration versions."""
    return ConfigurationVersion(
        id=id,
        version=version,
        contentUrl=content_url,
        sha256sum=sha256sum,
        valid_from=valid_from,
    )


class TestConfiguration:
    """Test suite for the Configuration class."""

    def create_event(
        self,
        id: str = "test-config",
        version: int = 1,
        valid_from: Optional[str] = None,
        sha256sum: str = "test-hash",
        content_url: str = "http://example.com/config",
    ) -> Event:
        """Helper method to create test events."""
        return {
            "id": id,
            "event": "created",
            "contentUrl": content_url,
            "metadata": {
                "type": "test-type",
                "target_key": "test-key",
                "valid_from": valid_from,
                "category": "test-category",
                "version": version,
                "created_at": "2025-01-01T00:00:00Z",
                "sha256sum": sha256sum,
            },
        }

    def test_from_event(self):
        """Test creating a Configuration from an Event."""
        event = self.create_event(version=1, valid_from="2025-01-01T12:00:00Z")
        config = Configuration.from_event(event)

        assert len(config.versions) == 1
        assert 1 in config.versions
        assert config.versions[1].id == "test-config"
        assert config.versions[1].version == 1
        assert config.version is None
        assert config.next_version is None
        assert config.previous_version is None

    def test_add_version(self):
        """Test adding versions to a Configuration."""
        config = Configuration(versions={})

        # Add first version
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        config.add_version(version1)

        assert len(config.versions) == 1
        assert 1 in config.versions
        assert config.versions[1] == version1

        # Add second version
        version2 = create_configuration_version(version=2, valid_from=2000.0)
        config.add_version(version2)

        assert len(config.versions) == 2
        assert 2 in config.versions
        assert config.versions[2] == version2

    def test_add_version_clears_cache(self):
        """Test that adding a version clears the cached versions."""
        config = Configuration(versions={})
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        config.add_version(version1)

        # Set some cached values
        config.version = version1
        config.next_version = version1
        config.previous_version = version1

        # Add another version
        version2 = create_configuration_version(version=2, valid_from=2000.0)
        config.add_version(version2)

        # Cache should be cleared
        assert config.version is None
        assert config.next_version is None
        assert config.previous_version is None

    def test_find_valid_version_empty_versions(self):
        """Test finding valid version when no versions exist."""
        config = Configuration(versions={})
        result = config.find_valid_version(1500)
        assert result is None

    def test_find_valid_version_single_version_no_timestamp(self):
        """Test finding valid version with a single version that has no valid_from timestamp."""
        version = create_configuration_version(version=1, valid_from=0)
        config = Configuration(versions={1: version})

        result = config.find_valid_version(1500)
        assert result == version
        assert config.version == version
        assert config.previous_version is None
        assert config.next_version is None

    def test_find_valid_version_single_version_with_timestamp(self):
        """Test finding valid version with a single version that has a valid_from timestamp."""
        version = create_configuration_version(version=1, valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Timestamp after valid_from
        result = config.find_valid_version(1500)
        assert result == version

        # Timestamp before valid_from
        result = config.find_valid_version(500)
        assert result is None

    def test_find_valid_version_multiple_versions_chronological(self):
        """Test finding valid version with multiple versions in chronological order."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)
        version3 = create_configuration_version(version=3, valid_from=3000.0)

        config = Configuration(versions={1: version1, 2: version2, 3: version3})

        # Before any version
        result = config.find_valid_version(500)
        assert result is None

        # At first version
        result = config.find_valid_version(1000)
        assert result == version1
        assert config.version == version1
        assert config.previous_version is None
        assert config.next_version == version2

        # Between first and second
        result = config.find_valid_version(1500)
        assert result == version1

        # At second version
        result = config.find_valid_version(2000)
        assert result == version2
        assert config.version == version2
        assert config.previous_version == version1
        assert config.next_version == version3

        # Between second and third
        result = config.find_valid_version(2500)
        assert result == version2

        # At third version
        result = config.find_valid_version(3000)
        assert result == version3
        assert config.version == version3
        assert config.previous_version == version2
        assert config.next_version is None

        # After third version
        result = config.find_valid_version(4000)
        assert result == version3

    def test_find_valid_version_cache_invalidation_forward_time(self):
        """Test cache invalidation when time moves forward and next version becomes valid."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)

        config = Configuration(versions={1: version1, 2: version2})

        # First call at timestamp 1500 - should cache version1 as current, version2 as next
        result = config.find_valid_version(1500)
        assert result == version1
        assert config.version == version1
        assert config.next_version == version2

        # Second call at timestamp 2500 - next version should now be current
        result = config.find_valid_version(2500)
        assert result == version2
        assert config.version == version2
        assert config.previous_version == version1
        assert config.next_version is None

    def test_find_valid_version_out_of_order_messages(self):
        """Test handling out-of-order messages (timestamp going backwards)."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)

        config = Configuration(versions={1: version1, 2: version2})

        # First call at timestamp 2500 - should get version2
        result = config.find_valid_version(2500)
        assert result == version2
        assert config.version == version2
        assert config.previous_version == version1
        assert config.next_version is None

        # Second call at timestamp 1500 (backwards in time) - should get version1
        result = config.find_valid_version(1500)
        assert result == version1
        assert config.version == version2
        assert config.previous_version == version1
        assert config.next_version is None

    def test_find_valid_version_mixed_timestamp_and_no_timestamp(self):
        """Test finding valid version with mix of timestamped and non-timestamped versions."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=0.0)
        version3 = create_configuration_version(version=3, valid_from=3000.0)

        config = Configuration(versions={1: version1, 2: version2, 3: version3})

        # Non-timestamped version should be preferred as current
        result = config.find_valid_version(800)
        assert result == version2
        assert config.version == version2
        assert config.previous_version == None
        assert config.next_version == version3

        result = config.find_valid_version(1000)
        assert result == version2
        assert config.version == version2
        assert config.previous_version == None
        assert config.next_version == version3

        result = config.find_valid_version(2000)
        assert result == version2
        assert config.version == version2

        result = config.find_valid_version(3000)
        assert result == version3
        assert config.version == version3
        assert config.previous_version == version2
        assert config.next_version is None

    def test_find_versions_internal_helper(self):
        """Test the internal _find_versions helper method."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)
        version3 = create_configuration_version(version=3, valid_from=3000.0)

        config = Configuration(versions={1: version1, 2: version2, 3: version3})

        # Test at timestamp 2500
        previous, current, next_ver = config._find_versions(2500)
        assert previous == version1
        assert current == version2
        assert next_ver == version3

        # Test at timestamp 500 (before all versions)
        previous, current, next_ver = config._find_versions(500)
        assert previous is None
        assert current is None
        assert next_ver == version1

        # Test at timestamp 4000 (after all versions)
        previous, current, next_ver = config._find_versions(4000)
        assert previous == version2
        assert current == version3
        assert next_ver is None

    def test_find_versions_with_no_timestamp_versions(self):
        """Test _find_versions with versions that have no valid_from timestamp."""
        version1 = create_configuration_version(version=1, valid_from=0.0)
        version2 = create_configuration_version(version=2, valid_from=0.0)

        config = Configuration(versions={1: version1, 2: version2})

        # Should process in descending order, so version2 becomes current, version1 becomes previous
        previous, current, next_ver = config._find_versions(1500)
        assert previous is None
        assert current == version2
        assert next_ver is None

    def test_edge_case_exact_timestamp_match(self):
        """Test edge case where timestamp exactly matches valid_from."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)

        config = Configuration(versions={1: version1, 2: version2})

        # Exact match should return the version
        result = config.find_valid_version(2000)
        assert result == version2

    def test_version_ordering_with_gaps(self):
        """Test version handling with non-consecutive version numbers."""
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version5 = create_configuration_version(version=5, valid_from=2000.0)
        version10 = create_configuration_version(version=10, valid_from=3000.0)

        config = Configuration(versions={1: version1, 5: version5, 10: version10})

        result = config.find_valid_version(2500)
        assert result == version5
        assert config.version == version5
        assert config.previous_version == version1
        assert config.next_version == version10


class TestConfigurationVersion:
    """Test suite for the ConfigurationVersion class."""

    def test_from_event_with_timestamp(self) -> None:
        """Test creating ConfigurationVersion from Event with valid_from timestamp."""
        event: Event = {
            "id": "test-config",
            "event": "created",
            "contentUrl": "http://example.com/config",
            "metadata": {
                "type": "test-type",
                "target_key": "test-key",
                "valid_from": "2025-01-01T12:00:00Z",
                "category": "test-category",
                "version": 1,
                "created_at": "2025-01-01T00:00:00Z",
                "sha256sum": "test-hash",
            },
        }

        version = ConfigurationVersion.from_event(event)

        assert version.id == "test-config"
        assert version.version == 1
        assert version.contentUrl == "http://example.com/config"
        assert version.sha256sum == "test-hash"
        assert version.valid_from is not None
        # Should be converted to milliseconds
        expected_timestamp = datetime.strptime(
            "2025-01-01T12:00:00Z", "%Y-%m-%dT%H:%M:%S%z"
        )
        assert version.valid_from == expected_timestamp.timestamp() * 1000

    def test_from_event_without_timestamp(self) -> None:
        """Test creating ConfigurationVersion from Event without valid_from timestamp."""
        event: Event = {
            "id": "test-config",
            "event": "created",
            "contentUrl": "http://example.com/config",
            "metadata": {
                "type": "test-type",
                "target_key": "test-key",
                "valid_from": None,
                "category": "test-category",
                "version": 1,
                "created_at": "2025-01-01T00:00:00Z",
                "sha256sum": "test-hash",
            },
        }

        version = ConfigurationVersion.from_event(event)

        assert version.id == "test-config"
        assert version.version == 1
        assert version.valid_from == 0

    def test_success_method(self):
        """Test the success method resets retry parameters."""
        version = ConfigurationVersion(
            id="test",
            version=1,
            contentUrl="http://example.com",
            sha256sum="hash",
            valid_from=1000.0,
        )

        # Simulate some failed attempts
        version.failed()
        version.failed()
        assert version.retry_count == 2
        assert version.retry_at < sys.maxsize

        # Call success
        version.success()
        assert version.retry_count == 0
        assert version.retry_at == sys.maxsize

    def test_failed_method_exponential_backoff(self):
        """Test the failed method implements exponential backoff."""
        version = ConfigurationVersion(
            id="test",
            version=1,
            contentUrl="http://example.com",
            sha256sum="hash",
            valid_from=1000.0,
        )

        start = int(time.time())

        # First failure
        version.failed()
        assert version.retry_count == 1
        assert version.retry_at >= start + 1

        # Second failure
        version.failed()
        assert version.retry_count == 2
        assert version.retry_at >= start + 2  # Should be longer delay

        # Third failure
        version.failed()
        assert version.retry_count == 3
        assert version.retry_at >= start + 4  # Should be even longer delay

    def test_frozen_dataclass_immutability(self):
        """Test that ConfigurationVersion is properly frozen."""
        version = ConfigurationVersion(
            id="test",
            version=1,
            contentUrl="http://example.com",
            sha256sum="hash",
            valid_from=1000.0,
        )

        # Should not be able to modify frozen fields
        with pytest.raises((AttributeError, TypeError)):
            version.id = "modified"  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            version.version = 2  # type: ignore

        # But should be able to modify non-frozen fields via the methods
        version.failed()
        assert version.retry_count == 1


class TestLookupFindVersion:
    """Test suite for the Lookup._find_version method."""

    def test_find_version_exact_match(self, lookup):
        """Test finding version with exact type and on parameter match."""

        # Create a configuration version
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Set up configuration with specific type and key
        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Test exact match
        result = lookup._find_version("user-config", "user123", 1500)
        assert result == version

    def test_find_version_wildcard_fallback(self, lookup):
        """Test finding version using wildcard fallback when exact match not found."""

        # Create a configuration version for wildcard
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Set up wildcard configuration
        wildcard_config_id = lookup._config_id("user-config", "*")
        lookup._configurations[wildcard_config_id] = config

        # Test wildcard fallback when exact match doesn't exist
        result = lookup._find_version("user-config", "user123", 1500)
        assert result == version

    def test_find_version_exact_match_preferred_over_wildcard(self, lookup):
        """Test that exact match is preferred over wildcard when both exist."""

        # Create different versions for exact and wildcard
        exact_version = create_configuration_version(version=1, valid_from=1000.0)
        wildcard_version = create_configuration_version(version=2, valid_from=1000.0)

        exact_config = Configuration(versions={1: exact_version})
        wildcard_config = Configuration(versions={2: wildcard_version})

        # Set up both exact and wildcard configurations
        exact_config_id = lookup._config_id("user-config", "user123")
        wildcard_config_id = lookup._config_id("user-config", "*")
        lookup._configurations[exact_config_id] = exact_config
        lookup._configurations[wildcard_config_id] = wildcard_config

        # Test that exact match is preferred
        result = lookup._find_version("user-config", "user123", 1500)
        assert result == exact_version
        assert result != wildcard_version

    def test_find_version_no_configuration_found(self, lookup):
        """Test finding version when no configuration exists for type and key."""

        # No configurations set up
        result = lookup._find_version("user-config", "user123", 1500)
        assert result is None

    def test_find_version_no_valid_version_for_timestamp(self, lookup):
        """Test finding version when configuration exists but no valid version for timestamp."""

        # Create a configuration version that's valid after the requested timestamp
        version = create_configuration_version(valid_from=2000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Request timestamp before the version becomes valid
        result = lookup._find_version("user-config", "user123", 1500)
        assert result is None

    def test_find_version_multiple_types_different_keys(self, lookup):
        """Test finding version with different types and keys."""

        # Create configurations for different types
        user_version = create_configuration_version(version=1, valid_from=1000.0)
        admin_version = create_configuration_version(version=2, valid_from=1000.0)

        user_config = Configuration(versions={1: user_version})
        admin_config = Configuration(versions={2: admin_version})

        # Set up configurations with different types
        user_config_id = lookup._config_id("user-config", "user123")
        admin_config_id = lookup._config_id("admin-config", "admin456")
        lookup._configurations[user_config_id] = user_config
        lookup._configurations[admin_config_id] = admin_config

        # Test finding user config
        result = lookup._find_version("user-config", "user123", 1500)
        assert result == user_version

        # Test finding admin config
        result = lookup._find_version("admin-config", "admin456", 1500)
        assert result == admin_version

        # Test not finding cross-type config
        result = lookup._find_version("user-config", "admin456", 1500)
        assert result is None

    def test_find_version_wildcard_with_different_keys(self, lookup):
        """Test wildcard behavior with different specific keys."""

        # Create a wildcard configuration
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        wildcard_config_id = lookup._config_id("user-config", "*")
        lookup._configurations[wildcard_config_id] = config

        # Test that wildcard works for any key
        result1 = lookup._find_version("user-config", "user123", 1500)
        assert result1 == version

        result2 = lookup._find_version("user-config", "user456", 1500)
        assert result2 == version

        result3 = lookup._find_version("user-config", "any-key", 1500)
        assert result3 == version

    def test_find_version_config_id_generation(self, lookup):
        """Test that configuration ID generation works correctly."""

        # Test config ID generation
        config_id1 = lookup._config_id("user-config", "user123")
        config_id2 = lookup._config_id("user-config", "user456")
        config_id3 = lookup._config_id("admin-config", "user123")
        wildcard_id = lookup._config_id("user-config", "*")

        # All should be different
        assert len({config_id1, config_id2, config_id3, wildcard_id}) == 4

        # Same inputs should produce same ID
        assert config_id1 == lookup._config_id("user-config", "user123")

    def test_find_version_timestamp_boundary_conditions(self, lookup):
        """Test finding version at timestamp boundary conditions."""

        # Create a configuration version
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Test at exact timestamp
        result = lookup._find_version("user-config", "user123", 1000)
        assert result == version

        # Test just before
        result = lookup._find_version("user-config", "user123", 999)
        assert result is None

        # Test just after
        result = lookup._find_version("user-config", "user123", 1001)
        assert result == version

    def test_find_version_multiple_versions_time_selection(self, lookup):
        """Test finding correct version when multiple versions exist."""

        # Create multiple versions
        version1 = create_configuration_version(version=1, valid_from=1000.0)
        version2 = create_configuration_version(version=2, valid_from=2000.0)
        version3 = create_configuration_version(version=3, valid_from=3000.0)

        config = Configuration(versions={1: version1, 2: version2, 3: version3})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Test different timestamps select different versions
        result = lookup._find_version("user-config", "user123", 1500)
        assert result == version1

        result = lookup._find_version("user-config", "user123", 2500)
        assert result == version2

        result = lookup._find_version("user-config", "user123", 3500)
        assert result == version3

    def test_find_version_special_characters_in_keys(self, lookup):
        """Test finding version with special characters in type and key parameters."""

        # Create configuration with special characters
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Test with special characters
        special_type = "user-config:v2"
        special_key = "user@domain.com"

        config_id = lookup._config_id(special_type, special_key)
        lookup._configurations[config_id] = config

        result = lookup._find_version(special_type, special_key, 1500)
        assert result == version


class TestLookupJoin:
    """Test suite for the Lookup.join method."""

    def test_join_successful_enrichment(self, lookup):
        """Test successful enrichment with join method."""

        # Create a configuration version
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Set up configuration
        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Mock the content fetching to return test data
        test_content = {
            "name": "John Doe",
            "role": "admin",
            "permissions": ["read", "write"],
        }

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            # Define fields to extract
            fields = {
                "user_name": Field(type="user-config", jsonpath="$.name"),
                "user_role": Field(type="user-config", jsonpath="$.role"),
                "user_permissions": Field(
                    type="user-config",
                    jsonpath="$.permissions.[*]",
                    first_match_only=False,
                ),
            }

            # Prepare input data
            value = {"original_key": "original_value"}

            # Call join method
            lookup.join(
                fields=fields,
                on="user123",
                value=value,
                key="message_key",
                timestamp=1500,
                headers={},
            )

            # Verify enrichment
            assert value["user_name"] == "John Doe"
            assert value["user_role"] == "admin"
            assert value["user_permissions"] == ["read", "write"]
            assert value["original_key"] == "original_value"  # Original data preserved

    def test_join_with_default_values(self, lookup):
        """Test join method with default values when fields are missing."""

        # Create a configuration version
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Set up configuration
        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Mock the content fetching to return data missing some fields
        test_content = {"name": "John Doe"}  # Missing role and permissions

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            # Define fields with defaults
            fields = {
                "user_name": Field(type="user-config", jsonpath="$.name"),
                "user_role": Field(
                    type="user-config", jsonpath="$.role", default="guest"
                ),
                "user_permissions": Field(
                    type="user-config",
                    jsonpath="$.permissions",
                    default=[],
                    first_match_only=False,
                ),
            }

            # Prepare input data
            value = {}

            # Call join method
            lookup.join(
                fields=fields,
                on="user123",
                value=value,
                key="message_key",
                timestamp=1500,
                headers={},
            )

            # Verify enrichment with defaults
            assert value["user_name"] == "John Doe"
            assert value["user_role"] == "guest"  # Default value
            assert value["user_permissions"] == []  # Default value

    def test_join_with_missing_configuration(self, lookup):
        """Test join method when no configuration is found."""

        # No configurations set up

        # Define fields with defaults
        fields = {
            "user_name": Field(
                type="user-config", jsonpath="$.name", default="Unknown"
            ),
            "user_role": Field(type="user-config", jsonpath="$.role", default="guest"),
        }

        # Prepare input data
        value = {"original_key": "original_value"}

        # Call join method
        lookup.join(
            fields=fields,
            on="user123",
            value=value,
            key="message_key",
            timestamp=1500,
            headers={},
        )

        # Verify default values are used
        assert value["user_name"] == "Unknown"
        assert value["user_role"] == "guest"
        assert value["original_key"] == "original_value"

    def test_join_with_wildcard_fallback(self, lookup):
        """Test join method using wildcard fallback."""

        # Create a wildcard configuration
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        # Set up wildcard configuration
        wildcard_config_id = lookup._config_id("user-config", "*")
        lookup._configurations[wildcard_config_id] = config

        # Mock the content fetching
        test_content = {"default_role": "member", "default_quota": 100}

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            # Define fields
            fields = {
                "role": Field(type="user-config", jsonpath="$.default_role"),
                "quota": Field(type="user-config", jsonpath="$.default_quota"),
            }

            # Prepare input data
            value = {}

            # Call join method - should use wildcard since no exact match
            lookup.join(
                fields=fields,
                on="any_user",
                value=value,
                key="message_key",
                timestamp=1500,
                headers={},
            )

            # Verify wildcard configuration is used
            assert value["role"] == "member"
            assert value["quota"] == 100

    def test_join_with_multiple_types(self, lookup):
        """Test join method with fields from multiple configuration types."""

        # Create configurations for different types
        user_version = create_configuration_version(
            version=1, valid_from=1000.0, content_url="user"
        )
        admin_version = create_configuration_version(
            version=1, valid_from=1000.0, content_url="admin"
        )

        user_config = Configuration(versions={1: user_version})
        admin_config = Configuration(versions={2: admin_version})

        # Set up configurations
        user_config_id = lookup._config_id("user-config", "user123")
        admin_config_id = lookup._config_id("admin-config", "user123")
        lookup._configurations[user_config_id] = user_config
        lookup._configurations[admin_config_id] = admin_config

        # Mock content for different configurations
        user_content = {"name": "John Doe", "department": "Engineering"}
        admin_content = {"permissions": ["read", "write", "delete"], "level": "super"}

        def mock_fetch_content(version):
            if version.contentUrl == "user":  # user config
                return user_content
            elif version.contentUrl == "admin":  # admin config
                return admin_content
            return {}

        with patch.object(
            lookup, "_fetch_version_content", side_effect=mock_fetch_content
        ):
            # Define fields from different types
            fields = {
                "user_name": Field(type="user-config", jsonpath="$.name"),
                "department": Field(type="user-config", jsonpath="$.department"),
                "permissions": Field(
                    type="admin-config",
                    jsonpath="$.permissions",
                ),
                "admin_level": Field(type="admin-config", jsonpath="$.level"),
            }

            # Prepare input data
            value = {}

            # Call join method
            lookup.join(
                fields=fields,
                on="user123",
                value=value,
                key="message_key",
                timestamp=1500,
                headers={},
            )

            # Verify data from both configurations
            assert value["user_name"] == "John Doe"
            assert value["department"] == "Engineering"
            assert value["permissions"] == ["read", "write", "delete"]
            assert value["admin_level"] == "super"

    def test_join_with_invalid_timestamp(self, lookup):
        """Test join method when timestamp is before configuration valid_from."""

        # Create a configuration version valid from timestamp 2000
        version = create_configuration_version(valid_from=2000.0)
        config = Configuration(versions={1: version})

        # Set up configuration
        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Define fields with defaults
        fields = {
            "user_name": Field(
                type="user-config", jsonpath="$.name", default="Unknown"
            ),
        }

        # Prepare input data
        value = {}

        # Call join method with timestamp before valid_from
        lookup.join(
            fields=fields,
            on="user123",
            value=value,
            key="message_key",
            timestamp=1500,  # Before valid_from (2000)
            headers={},
        )

        # Should use default since no valid version for timestamp
        assert value["user_name"] == "Unknown"

    def test_join_fields_caching(self, lookup):
        """Test that fields are cached by type for performance."""

        # Create a configuration
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        test_content = {"name": "John"}

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            fields = {
                "user_name": Field(type="user-config", jsonpath="$.name"),
            }

            value1 = {}
            value2 = {}

            # First call should cache the fields
            lookup.join(
                fields=fields,
                on="user123",
                value=value1,
                key="key",
                timestamp=1500,
                headers={},
            )

            # Verify fields are cached
            assert id(fields) in lookup._fields_by_type

            # Second call should use cached fields
            lookup.join(
                fields=fields,
                on="user123",
                value=value2,
                key="key",
                timestamp=1500,
                headers={},
            )

            # Both calls should work
            assert value1 == value2

    def test_join_version_retry_logic(self, lookup):
        """Test join method handles version retry logic correctly."""

        # Create a configuration version
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Set version to failed state with retry_at in the past
        version.failed()
        # Use __setattr__ to modify the immutable field
        super(ConfigurationVersion, version).__setattr__(
            "retry_at", int(time.time()) - 100
        )

        with patch.object(lookup._version_data_cached, "remove") as mock_remove:
            with patch.object(
                lookup, "_fetch_version_content", return_value={"name": "John"}
            ) as mock_fetch:
                fields = {
                    "user_name": Field(type="user-config", jsonpath="$.name"),
                }

                value = {}

                # Call join method
                lookup.join(
                    fields=fields,
                    on="user123",
                    value=value,
                    key="message_key",
                    timestamp=1500,
                    headers={},
                )

                # Verify cache was cleared for retry
                mock_remove.assert_called_once()
                mock_fetch.assert_called_once()

                assert value == {"user_name": "John"}

    def test_join_preserves_or_overrides_original_data(self, lookup):
        """Test that join method preserves original data in value dict."""

        # Create a configuration
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        test_content = {"name": "John", "role": "developer"}

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            fields = {
                "user_name": Field(type="user-config", jsonpath="$.name"),
                "user_role": Field(type="user-config", jsonpath="$.role"),
            }

            # Prepare input data with existing values
            value = {
                "message_id": "msg123",
                "original_data": {"key": "value"},
                "timestamp": 1234567890,
                "user_name": "Dave",  # Existing value to be overridden
            }

            # Call join method
            lookup.join(
                fields=fields,
                on="user123",
                value=value,
                key="message_key",
                timestamp=1500,
                headers={},
            )

            # Verify original data is preserved
            assert value["message_id"] == "msg123"
            assert value["original_data"] == {"key": "value"}
            assert value["timestamp"] == 1234567890

            # Verify new data is added
            assert value["user_name"] == "John"
            assert value["user_role"] == "developer"

    def test_join_with_error_fields_no_default(self, lookup):
        """Test join method when fields have no default and content is missing."""

        # Create a configuration
        version = create_configuration_version(valid_from=1000.0)
        config = Configuration(versions={1: version})

        config_id = lookup._config_id("user-config", "user123")
        lookup._configurations[config_id] = config

        # Mock content that's missing the required field
        test_content = {"other_field": "value"}

        with patch.object(lookup, "_fetch_version_content", return_value=test_content):
            fields = {
                "user_name": Field(
                    type="user-config", jsonpath="$.name"
                ),  # No default, will raise
            }

            value = {}

            # Should raise error when field is missing and no default
            with pytest.raises(KeyError, match="No match found for path"):
                lookup.join(
                    fields=fields,
                    on="user123",
                    value=value,
                    key="message_key",
                    timestamp=1500,
                    headers={},
                )
