import logging
import sys
import time
from typing import Optional

try:
    import neo4j
    from neo4j._sync.driver import Driver
    from neo4j.exceptions import (
        DatabaseUnavailable,
        ServiceUnavailable,
        SessionExpired,
        TransactionError,
    )
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[neo4j]" to use Neo4jSink'
    ) from exc

from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

__all__ = ("Neo4jSink",)

logger = logging.getLogger(__name__)


_ATTEMPT_BACKOFF = 5
_SINK_BACKOFF = 30


class Neo4jSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        cypher_query: str,
        chunk_size: int = 10000,
        **kwargs,
    ) -> None:
        """
        A connector to sink processed data to Neo4j.

        :param host: The Neo4j database hostname.
        :param port: The Neo4j database port.
        :param username: The Neo4j database username.
        :param password: The Neo4j database password.
        :param cypher_query: A Cypher Query to execute on each record.
            Behavior attempts to match other Neo4j connectors:
            - Uses "dot traversal" for (nested) dict key access; ex: "col_x.col_y.col_z"
            - Message value is bound to the alias "event"; ex: "event.field_a".
            - Message key, value, header and timestamp are bound to "__{attr}"; ex: "__key".
        :param chunk_size: Adjust the size of a Neo4j transactional chunk.
            - This does NOT affect how many records can be written/flushed at once.
            - The chunks are committed only if ALL of them succeed.
            - Larger chunks are generally more efficient, but can encounter size issues.
            - This is only necessary to adjust when messages are especially large.
        :param kwargs: Additional keyword arguments passed to the
            `neo4j.GraphDatabase.driver` instance.

        Example Usage:

        ```
        from quixstreams import Application
        from quixstreams.sinks.community.neo4j import Neo4jSink

        app = Application(broker_address="localhost:9092")
        topic = app.topic("topic-name")

        # records structured as:
        # {"name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

        # This assumes the given City nodes exist.
        # Notice the use of "event" to reference the message value.
        # Could also do things like __key, or __value.name.first.
        cypher_query = '''
        MERGE (p:Person {first_name: event.name.first, last_name: event.name.last})
        SET p.age = event.age
        MERGE (c:City {name: event.city})
        MERGE (p)-[:LIVES_IN]->(c)
        '''

        # Configure the sink
        neo4j_sink = Neo4jSink(
            host="localhost",
            port=7687,
            username="neo4j",
            password="local_password",
            cypher_query=cypher_query,
        )

        sdf = app.dataframe(topic=topic)
        sdf.sink(neo4j_sink)

        if __name__ == "__main__":
            app.run()
        ```
        """

        super().__init__()
        self._credentials = {
            "uri": f"bolt://{host}:{port}",
            "auth": (username, password),
            **kwargs,
        }
        self._chunk_size = chunk_size
        self._query = self._make_cypher_query(cypher_query)
        self._client: Optional[Driver] = None

    def setup(self):
        self._client = neo4j.GraphDatabase.driver(**self._credentials)
        with self._client.session() as session:
            # confirms correct credentials
            for _ in session.run("RETURN 1 AS test"):
                pass

    def _make_cypher_query(self, user_query: str) -> str:
        """
        There are a few things going on here:

        This extends the user's query so the same references/bindings as mentioned in
        Neo4j's other connector docs can be used.

        Submitting all records at once with UNWIND $records also allows for more
        optimal transactions (treats it like a batch upload).

        NOTE: Cypher Queries allow for "dot" notation: the Neo4j driver can
        access traverse dictionary/json paths.
        """
        return (
            "UNWIND $records AS record\n"
            "WITH record.__value as __value, "
            "record.__key as __key, "
            "record.__header as __header, "
            "record.__timestamp as __timestamp, "
            "record.__value as event\n"
            f"{user_query}"
        )

    def _transaction(self, tx: neo4j.ManagedTransaction, batch: SinkBatch):
        """
        Encapsulating all tx.run() calls in a function means they are all part of
        the same transaction.
        """
        _start = time.monotonic()
        min_timestamp = sys.maxsize
        max_timestamp = -1
        for chunk in batch.iter_chunks(self._chunk_size):
            # Records must be a list of dicts to work in the batch Neo4j query.
            records = []
            for r in chunk:
                ts = r.timestamp
                record = {
                    "__key": r.key.decode() if isinstance(r.key, bytes) else r.key,
                    "__value": r.value,
                    "__header": r.headers or {},
                    "__timestamp": ts,
                }
                records.append(record)
                min_timestamp = min(ts, min_timestamp)
                max_timestamp = max(ts, max_timestamp)
            tx.run(self._query, records=records)
        logger.info(
            f"Sent data to Neo4j; "
            f"messages_processed={batch.size} "
            f"min_timestamp={min_timestamp} "
            f"max_timestamp={max_timestamp} "
            f"time_elapsed={round(time.monotonic() - _start, 2)}s"
        )

    def write(self, batch: SinkBatch):
        attempts_remaining = 3
        with self._client.session() as session:
            while attempts_remaining:
                try:
                    session.execute_write(self._transaction, batch)
                    return
                except (
                    SessionExpired,
                    ServiceUnavailable,
                    DatabaseUnavailable,
                    TransactionError,
                ) as e:
                    logger.error(f"Failed to commit Neo4j transaction; error: {e}")
                    attempts_remaining -= 1
                    if attempts_remaining:
                        logger.error(
                            f"Sleeping for {_ATTEMPT_BACKOFF} seconds and re-attempting. "
                            f"Attempts remaining: {attempts_remaining}"
                        )
                        time.sleep(_ATTEMPT_BACKOFF)
            logger.error(
                "Consecutive Neo4j write attempts failed; "
                f"performing a longer backoff of {_SINK_BACKOFF} seconds..."
            )
            raise SinkBackpressureError(retry_after=_SINK_BACKOFF)
