import logging

try:
    import neo4j
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


class Neo4jSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        cypher_query: str,
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
        self._uri = f"bolt://{host}:{port}"
        self._client = neo4j.GraphDatabase.driver(
            self._uri,
            auth=(username, password),
            **kwargs,
        )
        self._query = self._make_cypher_query(cypher_query)
        print(f"final cypher query:\n{self._query}")

    def _make_cypher_query(self, user_query: str):
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

    def write(self, sink_batch: SinkBatch):
        """
        Cypher query to create a Person node and relate it to a City node.
        """
        # Records must be a list of dicts to work in the batch Neo4j query.
        records = [
            {
                "__key": r.key.decode() if isinstance(r.key, bytes) else r.key,
                "__value": r.value,
                "__header": r.headers or {},
                "__timestamp": r.timestamp,
            }
            for r in sink_batch
        ]
        with self._client.session() as session:
            try:
                session.write_transaction(
                    lambda tx: tx.run(self._query, records=records)
                )
            except (
                SessionExpired,
                ServiceUnavailable,
                DatabaseUnavailable,
                TransactionError,
            ) as e:
                logger.error(
                    f"Failed to commit Neo4j transaction; error: {e}"
                    "\nBacking off and retrying with a new Neo4j transaction"
                )
                raise SinkBackpressureError
        logger.info("WRITE SUCCESS!")
