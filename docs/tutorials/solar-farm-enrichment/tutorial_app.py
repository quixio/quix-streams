import os
import random
import time
from datetime import datetime, timedelta

from quixstreams import Application
from quixstreams.sources import Source

LOCATION_ID = "location-1"
PANELS_IDS = ["panel-1", "panel-2", "panel-3"]


def timestamp_to_str(timestamp: float) -> str:
    return datetime.utcfromtimestamp(timestamp).isoformat()


class WeatherForecastGenerator(Source):
    """
    A Quix Streams Source that generates weather forecast data.
    Emits new configuration settings every 30 seconds.
    """

    def generate_forecast(self) -> dict:
        """Generate a new data point"""
        # Generate random values within specified ranges
        forecast_temp = random.randint(15, 35)
        forecast_cloud = random.randint(0, 100)
        timestamp = time.time()
        return {
            "timestamp": timestamp,
            "forecast_temp": forecast_temp,
            "forecast_cloud": forecast_cloud,
        }

    def run(self):
        """
        Generate weather forecast data every 30 seconds
        """

        next_forecast_at = time.time()

        while self.running:
            if time.time() < next_forecast_at:
                time.sleep(1)
                continue

            # Generate next configuration
            forecast = self.generate_forecast()

            # Serialize and produce the forecast for the location
            message = self.serialize(
                key=LOCATION_ID,
                value=forecast,
                timestamp_ms=int(forecast["timestamp"] * 1000),
            )
            self.produce(
                key=message.key,
                value=message.value,
                timestamp=message.timestamp,
            )
            print(
                f"Forecast generated "
                f"at time {timestamp_to_str(forecast['timestamp'])}"
            )

            # Schedule next forecast in 30s
            next_forecast_at = time.time() + 30


class BatteryTelemetryGenerator(Source):
    """
    A Quix Streams Source that generates battery telemetry data for solar panels.
    It cycles through the panel IDs, generating random  power output and internal temperature values.
    It emits new telemetry events every second.
    """

    def generate_telemetry_event(self, panel_id: str) -> dict:
        """
        Generate a telemetry event for the solar panel.
        """
        event = {
            "timestamp": time.time(),
            "panel_id": panel_id,
            "location_id": LOCATION_ID,
            "temperature_C": random.randint(15, 35),
            "power_watt": random.randint(2, 10) / 10,
        }
        return event

    def run(self):
        while self.running:
            for panel_id in PANELS_IDS:
                event = self.generate_telemetry_event(panel_id=panel_id)
                message = self.serialize(
                    key=LOCATION_ID,
                    value=event,
                    timestamp_ms=int(event["timestamp"] * 1000),
                )
                print(
                    f'Producing telemetry event for panel "{panel_id}" '
                    f'at {timestamp_to_str(event["timestamp"])}"'
                )
                self.produce(
                    key=message.key, value=message.value, timestamp=message.timestamp
                )
            time.sleep(1)


def main():
    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
        consumer_group="solar-farm",
        auto_offset_reset="earliest",
        # Disable changelog topics for this app, but it's recommended to keep them "on" in production
        use_changelog_topics=False,
    )
    output_topic = app.topic(name="telemetry-with-forecast")

    telemetry_sdf = app.dataframe(source=BatteryTelemetryGenerator(name="telemetry"))
    forecast_sdf = app.dataframe(source=WeatherForecastGenerator(name="forecast"))

    def merge_events(telemetry: dict, forecast: dict) -> dict:
        """
        Merge the matching events into a new one
        """
        forecast = {"forecast." + k: v for k, v in forecast.items()}
        return {**telemetry, **forecast}

    # Join the telemetry data with the latest effective forecasts (forecast timestamp always <= telemetry timestamp)
    # using join_asof()
    enriched_sdf = telemetry_sdf.join_asof(
        forecast_sdf,
        how="inner",  # Join using "inner" strategy
        on_merge=merge_events,  # Use a custom function to merge events together because of the overlapping keys
        grace_ms=timedelta(days=7),  # Store forecast updates in state for 7d
    )

    # Convert timestamps to strings for readbility
    enriched_sdf["timestamp"] = enriched_sdf["timestamp"].apply(timestamp_to_str)
    enriched_sdf["forecast.timestamp"] = enriched_sdf["forecast.timestamp"].apply(
        timestamp_to_str
    )

    # Print the enriched data
    enriched_sdf.print_table(live=False)

    # Produce results to the output topic
    enriched_sdf.to_topic(output_topic)

    # Start the application
    app.run()


if __name__ == "__main__":
    main()
