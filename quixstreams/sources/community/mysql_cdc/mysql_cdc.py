import logging
import time
from typing import Optional

from quixstreams.sources.base import StatefulSource

from .mysql_helper import MySqlHelper

__all__ = ("MySqlCdcSource",)

logger = logging.getLogger(__name__)


class MySqlCdcSource(StatefulSource):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        state_dir: str = "state",
        initial_snapshot: bool = False,
        snapshot_host: Optional[str] = None,
        snapshot_batch_size: int = 1000,
        force_snapshot: bool = False,
        name: str = "mysql-cdc-source",
    ):
        super().__init__(name=name)

        self._database = database
        self._table = table
        self._table_name = f"{self._database}.{self._table}"
        self.wait_interval = 0.1
        self._helper = MySqlHelper(
            **{
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
                "table": table,
                "snapshot_host": snapshot_host or host,
                "state_dir": state_dir,
            }
        )

        self.initial_snapshot = initial_snapshot
        self.snapshot_batch_size = snapshot_batch_size
        self.force_snapshot = force_snapshot

        # Connection objects - will be initialized in setup()
        self.conn = None
        self.binlog_stream = None

        # Message buffering
        self.buffer = []
        self.last_flush_time = time.time()
        self.flush_interval = 0.5  # 500ms

    def setup(self):
        """Initialize MySQL connection and CDC setup"""
        try:
            self._helper.enable_binlog_if_needed()
            self._helper.setup_mysql_cdc()
            self.conn = self._helper.connect_mysql()
            self.binlog_stream = self._helper.create_binlog_stream()
            logger.info("MySQL CDC CONNECTED!")
        except Exception as e:
            logger.error(f"ERROR during MySQL CDC setup - {e}")
            raise

    def is_snapshot_completed(self):
        """Check if initial snapshot has been completed using state store"""
        snapshot_key = f"snapshot_completed_{self._database}_{self._table}"
        return self.state.get(snapshot_key, False) and not self.force_snapshot

    def mark_snapshot_completed(self):
        """Mark initial snapshot as completed in state store"""
        snapshot_key = f"snapshot_completed_{self._database}_{self._table}"
        snapshot_info = {
            "completed_at": time.time(),
            "schema": self._database,
            "table": self._table,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        }
        self.state.set(snapshot_key, True)
        self.state.set(f"snapshot_info_{self._database}_{self._table}", snapshot_info)
        logger.info(f"Snapshot completion marked in state store for {self._table_name}")

    def get_snapshot_info(self):
        """Get information about when snapshot was completed"""
        info_key = f"snapshot_info_{self._database}_{self._table}"
        return self.state.get(info_key, None)

    def save_binlog_position(self, log_file, log_pos):
        """Save binlog position to state store"""
        binlog_key = f"binlog_position_{self._database}_{self._table}"
        position_info = {
            "log_file": log_file,
            "log_pos": log_pos,
            "timestamp": time.time(),
        }
        self.state.set(binlog_key, position_info)

    def get_binlog_position(self):
        """Get saved binlog position from state store"""
        binlog_key = f"binlog_position_{self._database}_{self._table}"
        return self.state.get(binlog_key, None)

    def perform_initial_snapshot_if_needed(self):
        """Perform initial snapshot if enabled and not already completed"""
        if not self.initial_snapshot:
            logger.info("Initial snapshot is disabled - starting CDC stream only")
            return

        if self.is_snapshot_completed():
            snapshot_info = self.get_snapshot_info()
            if self.force_snapshot:
                logger.info(
                    "Initial snapshot already completed but FORCE_SNAPSHOT=true - performing snapshot again..."
                )
            else:
                logger.info(
                    f"Initial snapshot already completed at {snapshot_info.get('timestamp', 'unknown time')} - skipping"
                )
                return
        else:
            logger.info(
                "Initial snapshot is enabled and not yet completed - performing snapshot..."
            )

        if not self.is_snapshot_completed() or self.force_snapshot:
            try:
                snapshot_changes = self._helper.perform_initial_snapshot(
                    self.snapshot_batch_size
                )

                # Send snapshot data to Kafka immediately
                for change in snapshot_changes:
                    msg = self.serialize(key=self._table_name, value=change)
                    self.produce(
                        key=msg.key,
                        value=msg.value,
                    )

                # Flush to ensure all snapshot data is sent and commit state
                self.flush()
                logger.info(
                    f"Initial snapshot completed - {len(snapshot_changes)} records sent to Kafka"
                )

                # Mark snapshot as completed
                self.mark_snapshot_completed()
                # Flush again to save the snapshot completion state
                self.flush()

            except Exception as e:
                logger.error(f"Failed to perform initial snapshot: {e}")
                raise

    def process_buffered_messages(self):
        """Process and send buffered messages if flush interval has passed"""
        current_time = time.time()

        if (current_time - self.last_flush_time) >= self.flush_interval and len(
            self.buffer
        ) > 0:
            logger.debug(f"Processing {len(self.buffer)} buffered messages")

            # Send all buffered messages
            for message in self.buffer:
                msg = self.serialize(key=self._table_name, value=message)
                self.produce(
                    key=msg.key,
                    value=msg.value,
                )

            # Save binlog position if available
            if hasattr(self.binlog_stream, "log_file") and hasattr(
                self.binlog_stream, "log_pos"
            ):
                self.save_binlog_position(
                    self.binlog_stream.log_file, self.binlog_stream.log_pos
                )

            # Flush the producer and commit state changes
            self.flush()

            # Clear the buffer and update flush time
            self.buffer = []
            self.last_flush_time = current_time

            logger.debug("Buffered messages sent and state committed")

    def run(self):
        """Main CDC loop - runs while self.running is True"""
        logger.info(f"Starting MySQL CDC source for {self._table_name}")

        # Perform initial snapshot if needed
        self.perform_initial_snapshot_if_needed()

        # Log binlog position if available
        saved_position = self.get_binlog_position()
        if saved_position:
            logger.info(f"Resuming from binlog position: {saved_position}")

        # Start CDC loop
        while self.running:
            try:
                # Get changes from MySQL binlog
                changes = self._helper.get_changes(self.binlog_stream)

                # Add changes to buffer
                for change in changes:
                    self.buffer.append(change)

                if len(self.buffer) > 0:
                    logger.debug(f"Buffer length: {len(self.buffer)}")

                # Process buffered messages if flush interval has passed
                self.process_buffered_messages()

                # Small sleep to prevent excessive CPU usage
                time.sleep(self.wait_interval)

            except Exception as e:
                logger.error(f"Error in CDC loop: {e}")
                # Still continue running unless it's a fatal error
                time.sleep(1)  # Wait a bit longer on error

    def stop(self):
        """Clean up resources when stopping"""
        logger.info("Stopping MySQL CDC source")

        # Process any remaining buffered messages
        if len(self.buffer) > 0:
            logger.info(f"Processing {len(self.buffer)} remaining buffered messages")
            self.process_buffered_messages()

        # Clean up connections
        if self.conn:
            self.conn.close()
            logger.info("MySQL connection closed")

        if self.binlog_stream:
            self.binlog_stream.close()
            logger.info("Binlog stream closed")

        super().stop()
