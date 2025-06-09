def stream_id_from_strings(*strings: str) -> str:
    parts = sorted(set(strings))
    return "--".join(parts)
