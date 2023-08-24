import ctypes
from typing import Union, List

from .kafkaheader import KafkaHeader
from .topicpartitionoffset import TopicPartitionOffset
from .kafkatimestamp import KafkaTimestamp
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai, Array

from ..native.Python.QuixStreamsKafka.KafkaMessage import KafkaMessage as kmi


@nativedecorator
class KafkaMessage(object):
    """
    The message consumed from topic without any transformation.
    """

    def __init__(self,
                 key: Union[bytes, bytearray] = None,
                 value: Union[bytes, bytearray] = None,
                 headers: List[KafkaHeader] = None,
                 timestamp: KafkaTimestamp = None,
                 **kwargs):
        """
        Initializes a new instance of KafkaMessage.

        Args:
            key: The key of the message as bytes
            value: The value of the message as bytes
            headers: The optional headers of the message
        """

        self._value = None
        self._key = None
        self._timestamp = None
        self._headers = None
        self._topic_partition_offset = None


        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = kmi(net_pointer)
            return

        if value is None:
            raise ValueError(f"Parameter 'value' must not be none.")

        if not isinstance(value, (bytes, bytearray)):
            raise TypeError(f"Parameter 'value' has incorrect type '{type(value)}. Must be bytes or bytearray.")

        self._value = value
        value_uptr = ai.WriteBytes(value)

        key_uptr = None
        if key is not None:
            if not isinstance(key, (bytes, bytearray)):
                raise TypeError(f"Parameter 'key' has incorrect type '{type(key)}'. Must be bytes or bytearray.")

            key_uptr = ai.WriteBytes(key)
            self._key = key

        headers_ptr = None
        if headers is not None:
            pointers = [i.get_net_pointer() for i in headers]
            headers_ptr = ai.WritePointers(pointers)

            self._headers = headers

        timestamp_ptr = None
        if timestamp is not None:
            if not isinstance(timestamp, KafkaTimestamp):
                raise TypeError(f"Parameter 'timestamp' has incorrect type '{type(timestamp)}'. Must be KafkaTimestamp.")

            timestamp_ptr = timestamp.get_net_pointer()
            self._timestamp = timestamp

        partition_offset_ptr = None

        net_pointer = kmi.Constructor(key_uptr, value_uptr, headers_ptr, timestamp_ptr, partition_offset_ptr)
        self._interop = kmi(net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the KafkaMessage instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the KafkaMessage instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def key(self) -> bytes:
        """
        Gets the optional key of the message. Depending on the broker and message, it is not guaranteed.

        Returns:
            bytes: The optional key of the message.
        """
        if self._key is None:
            keys_uptr = self._interop.get_Key()
            self._key = Array.ReadBytes(keys_uptr)

        return self._key

    @property
    def value(self) -> bytes:
        """
        Gets the message value (bytes content of the message).

        Returns:
            Union[bytearray, bytes]: The message value (bytes content of the message).
        """
        if self._value is None:
            val_uptr = self._interop.get_Value()
            self._value = ai.ReadBytes(val_uptr)

        return self._value

    @property
    def timestamp(self) -> KafkaTimestamp:
        """
        Gets the message timestamp.

        Returns:
            KafkaTimestamp: The message timestamp
        """
        if self._timestamp is None:
            ts_ptr = self._interop.get_Timestamp()
            self._timestamp = KafkaTimestamp(net_pointer=ts_ptr)

        return self._timestamp

    @property
    def headers(self) -> [KafkaHeader]:
        """
        Gets the read only message headers. Modification won't reflect, init new KafkaMessage for that

        Returns:
            [KafkaHeader]: The message headers. Read only, modification won't reflect, init new KafkaMessage for that
        """
        if self._headers is None:
            header_arr_uptr = self._interop.get_Headers()
            header_hptrs = ai.ReadPointers(header_arr_uptr)
            if header_hptrs is None:
                self._headers = []
            else:
                self._headers = [KafkaHeader(net_pointer=ptr) for ptr in header_hptrs]

        return self._headers

    @property
    def topic_partition_offset(self) -> TopicPartitionOffset:
        """
        Gets the message timestamp.

        Returns:
            KafkaTimestamp: The message timestamp
        """
        if self._topic_partition_offset is None:
            tpo_ptr = self._interop.get_TopicPartitionOffset()
            self._topic_partition_offset = TopicPartitionOffset(net_pointer=tpo_ptr)

        return self._topic_partition_offset
