from typing import Generator, Union

import pytest
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
)

from quixstreams.models import (
    Deserializer,
    SchemaRegistryClientConfig,
    Serializer,
)
from tests.utilities.containers import SchemaRegistryContainer


@pytest.fixture()
def schema_registry_client_config(
    schema_registry_container: SchemaRegistryContainer,
) -> SchemaRegistryClientConfig:
    return SchemaRegistryClientConfig(
        url=schema_registry_container.schema_registry_address
    )


@pytest.fixture()
def schema_registry_client(
    schema_registry_client_config: SchemaRegistryClientConfig,
) -> SchemaRegistryClient:
    return SchemaRegistryClient(
        schema_registry_client_config.as_dict(plaintext_secrets=True)
    )


@pytest.fixture(autouse=True)
def _clear_schema_registry(
    schema_registry_client: SchemaRegistryClient,
) -> Generator[None, None, None]:
    # This will delete all schemas from the Schema Registry.
    # However, note that it will not reset the schema ID counter.
    # To restart schema IDs from 1, a container restart is required,
    # which would significantly increase testing times.
    yield
    while subjects := schema_registry_client.get_subjects():
        for subject in subjects:
            try:
                schema_registry_client.delete_subject(subject, permanent=True)
            except Exception as exc:
                if exc.error_code == 42206:
                    # One or more references exist to the schema. Skip for now.
                    continue
                raise


@pytest.fixture()
def _inject_schema_registry(
    request: pytest.FixtureRequest,
    schema_registry_client_config: SchemaRegistryClientConfig,
) -> Union[Deserializer, Serializer]:
    return request.param(schema_registry_client_config=schema_registry_client_config)


# This trick helps point multiple indirect attributes to a single fixture.
deserializer = serializer = _inject_schema_registry
