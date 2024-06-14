from typing import Optional

from quixstreams.models.types import MessageHeadersTuples, MessageHeadersMapping

__all__ = ("merge_headers",)


def merge_headers(
    original: Optional[MessageHeadersTuples], other: MessageHeadersMapping
) -> MessageHeadersTuples:
    """
    Merge two sets of Kafka message headers, overwriting headers in "origin"
    by the values from "other".

    :param original: original headers as a list of (key, value) tuples.
    :param other: headers to merge as a dictionary.
    :return: a list of (key, value) tuples.
    """
    if not other:
        return original or []
    elif not original:
        return list(other.items())
    elif not isinstance(original, (list, tuple)):
        raise ValueError("Headers must be either a list or tuple")

    # Make a shallow copy of "other" to pop keys from it
    other = other.copy()
    new_headers = []
    # Iterate over original headers and put them to a new list with values from
    # the "other" dict if the key is found
    for header, value in original:
        if header in other:
            value = other.pop(header)
        new_headers.append((header, value))
    # Append the new headers to the list
    new_headers.extend(other.items())
    return new_headers
