from datetime import datetime, timezone

__all__ = ("format_timestamp",)


def format_timestamp(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]
