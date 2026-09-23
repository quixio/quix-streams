"""MA Streaming Open Data v1 deserializer package.

Exports :class:`MAStreamingDeserializer`, a ``ProtobufDeserializer``
subclass that decodes both the outer ``Packet`` envelope and the inner
typed payload in one call.

The ``__protocol_version__`` constant matches the ``v1`` suffix of the
bundled schema's protobuf package (``ma.streaming.open_data.v1``).
"""

from .deserializer import MAStreamingDeserializer

__protocol_version__ = "v1"

__all__ = ("MAStreamingDeserializer", "__protocol_version__")
