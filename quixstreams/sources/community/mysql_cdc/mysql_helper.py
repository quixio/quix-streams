import base64
import json
import logging
import os
import time

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

__all__ = ("MySqlHelper",)

logger = logging.getLogger(__name__)


class MySqlHelper:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        state_dir: str,
        snapshot_host: str,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._snapshot_host = snapshot_host
        self._state_dir = state_dir

    def connect_mysql(self, override_host=None):
        return pymysql.connect(
            host=override_host or self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
        )

    def enable_binlog_if_needed(self):
        """Check and enable binary logging if not already enabled"""
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                # Check if binary logging is enabled
                cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
                result = cursor.fetchone()

                if result and result[1] == "ON":
                    logger.info("Binary logging is already enabled")
                else:
                    logger.warning(
                        "Binary logging is not enabled. Please enable it in MySQL configuration."
                    )
                    logger.warning("Add the following to your MySQL config:")
                    logger.warning("log-bin=mysql-bin")
                    logger.warning("binlog-format=ROW")
                    raise Exception("Binary logging must be enabled for CDC")

                # Check binlog format
                cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
                result = cursor.fetchone()

                if result and result[1] != "ROW":
                    logger.warning(
                        f"Binlog format is {result[1]}, should be ROW for CDC"
                    )
                    logger.warning(
                        "Please set binlog_format=ROW in MySQL configuration"
                    )

        finally:
            conn.close()

    def setup_mysql_cdc(self):
        """Setup MySQL for CDC - mainly validation"""
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLES LIKE '{self._table}'")
                if not cursor.fetchone():
                    raise Exception(f"Table {self._table} not found")
        finally:
            conn.close()

    def get_binlog_position_file(self):
        os.makedirs(self._state_dir, exist_ok=True)
        return os.path.join(
            self._state_dir, f"binlog_position_{self._database}_{self._table}.json"
        )

    def save_binlog_position(self, log_file, log_pos):
        position_file = self.get_binlog_position_file()
        position_data = {
            "log_file": log_file,
            "log_pos": log_pos,
            "timestamp": time.time(),
            "readable_time": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        }
        try:
            with open(position_file, "w") as f:
                json.dump(position_data, f, indent=2)
            logger.debug(f"Saved binlog position: {log_file}:{log_pos}")
        except Exception as e:
            logger.error(f"Failed to save binlog position: {e}")

    def load_binlog_position(self):
        position_file = self.get_binlog_position_file()
        if os.path.exists(position_file):
            try:
                with open(position_file, "r") as f:
                    position_data = json.load(f)
                logger.info(
                    f"Loaded binlog position: {position_data['log_file']}:{position_data['log_pos']} from {position_data.get('readable_time', 'unknown time')}"
                )
                return position_data["log_file"], position_data["log_pos"]
            except Exception as e:
                logger.error(f"Failed to load binlog position: {e}")
        return None, None

    def create_binlog_stream(self, server_id=1):
        mysql_settings = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "passwd": self._password,
        }

        log_file, log_pos = self.load_binlog_position()

        stream_kwargs = {
            "connection_settings": mysql_settings,
            "server_id": server_id,
            "only_events": [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            "resume_stream": True,
            "blocking": False,
        }

        # If we have a saved position, use it
        if log_file and log_pos:
            stream_kwargs["log_file"] = log_file
            stream_kwargs["log_pos"] = log_pos
            logger.info(
                f"Resuming binlog stream from saved position: {log_file}:{log_pos}"
            )
        else:
            logger.info(
                "No saved binlog position found, starting from current position"
            )

        return BinLogStreamReader(**stream_kwargs)

    def get_changes(self, stream):
        """Get changes from MySQL binlog stream and save position after processing"""
        changes = []
        last_position = None

        # Read available events (non-blocking)
        for binlogevent in stream:
            # Update position tracking
            if hasattr(stream, "log_file") and hasattr(stream, "log_pos"):
                last_position = (stream.log_file, stream.log_pos)

            # Filter by schema and table
            if (
                binlogevent.schema == self._database
                and binlogevent.table == self._table
            ):
                if isinstance(binlogevent, WriteRowsEvent):
                    # INSERT operation
                    for row in binlogevent.rows:
                        change = {
                            "kind": "insert",
                            "schema": binlogevent.schema,
                            "table": binlogevent.table,
                            "columnnames": list(row["values"].keys()),
                            "columnvalues": [
                                self.serialize_value(v) for v in row["values"].values()
                            ],
                            "oldkeys": {},
                        }
                        changes.append(change)

                elif isinstance(binlogevent, UpdateRowsEvent):
                    # UPDATE operation
                    for row in binlogevent.rows:
                        change = {
                            "kind": "update",
                            "schema": binlogevent.schema,
                            "table": binlogevent.table,
                            "columnnames": list(row["after_values"].keys()),
                            "columnvalues": [
                                self.serialize_value(v)
                                for v in row["after_values"].values()
                            ],
                            "oldkeys": {
                                "keynames": list(row["before_values"].keys()),
                                "keyvalues": [
                                    self.serialize_value(v)
                                    for v in row["before_values"].values()
                                ],
                            },
                        }
                        changes.append(change)

                elif isinstance(binlogevent, DeleteRowsEvent):
                    # DELETE operation
                    for row in binlogevent.rows:
                        change = {
                            "kind": "delete",
                            "schema": binlogevent.schema,
                            "table": binlogevent.table,
                            "columnnames": [],
                            "columnvalues": [],
                            "oldkeys": {
                                "keynames": list(row["values"].keys()),
                                "keyvalues": [
                                    self.serialize_value(v)
                                    for v in row["values"].values()
                                ],
                            },
                        }
                        changes.append(change)

        # Save position if we processed any events
        if last_position and changes:
            self.save_binlog_position(last_position[0], last_position[1])

        return changes

    def perform_initial_snapshot(self, batch_size=1000):
        conn = self.connect_mysql(override_host=self._snapshot_host)
        changes = []

        try:
            with conn.cursor() as cursor:
                # Get total row count for logging
                cursor.execute(
                    f"SELECT COUNT(*) FROM `{self._database}`.`{self._table}`"  # noqa: S608
                )
                total_rows = cursor.fetchone()[0]
                logger.info(
                    f"Starting initial snapshot of {self._database}.{self._table} - {total_rows} rows"
                )

                # Use LIMIT with OFFSET for batching to avoid memory issues with large tables
                offset = 0
                processed_rows = 0

                while True:
                    # Fetch batch of rows
                    cursor.execute(
                        f"SELECT * FROM `{self._database}`.`{self._table}` LIMIT {batch_size} OFFSET {offset}"  # noqa: S608
                    )
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]

                    # Convert each row to a change event
                    for row in rows:
                        # Convert row tuple to dictionary
                        row_dict = dict(zip(column_names, row))

                        # Convert values to JSON-serializable format
                        serialized_values = [
                            self.serialize_value(value) for value in row_dict.values()
                        ]

                        change = {
                            "kind": "snapshot_insert",  # Different kind to distinguish from real inserts
                            "schema": self._database,
                            "table": self._table,
                            "columnnames": column_names,
                            "columnvalues": serialized_values,
                            "oldkeys": {},
                        }
                        changes.append(change)

                    processed_rows += len(rows)
                    offset += batch_size

                    if processed_rows % 50000 == 0:  # Log progress every 50k rows
                        logger.info(
                            f"Snapshot progress: {processed_rows}/{total_rows} rows processed"
                        )

                logger.info(
                    f"Initial snapshot completed: {processed_rows} rows captured"
                )

        except Exception as e:
            logger.error(f"Error during initial snapshot: {e}")
            raise
        finally:
            conn.close()

        return changes

    @staticmethod
    def serialize_value(value):
        if value is None:
            return None
        elif isinstance(value, (bytes, bytearray)):
            return base64.b64encode(value).decode("utf-8")
        elif hasattr(value, "isoformat"):
            return value.isoformat()
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)
