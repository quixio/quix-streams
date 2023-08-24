import ctypes
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.ConfluentKafka.TopicPartitionOffset import TopicPartitionOffset as tpoi
from ..native.Python.ConfluentKafka.TopicPartition import TopicPartition as tpi
from ..native.Python.ConfluentKafka.Partition import Partition as pi
from ..native.Python.ConfluentKafka.Offset import Offset as oi



@nativedecorator
class Offset(object):
    """
    Represents a Kafka partition offset value.
    """

    def __init__(self, **kwargs):
        """
        Initializes a new instance of Offset.
        """

        self._value = None

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = oi(net_pointer)
            return

        raise TypeError(f"Offset does not support initialization")

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the offset instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the offset instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def value(self) -> int:
        """
        Gets the int value corresponding to this offset.

        Returns:
            int: The int value corresponding to this offset.
        """
        if self._value is None:
            self._value = self._interop.get_Value()

        return self._value

    @property
    def is_special(self) -> bool:
        return self.value == -1 or self.value == -2 or self.value == -1000 or self.value == -1011

    def __str__(self):
        if self.is_special:
            if self.value == -1:
                return "End [-1]"
            elif self.value == -2:
                return "Beginning [-2]"
            elif self.value == -1000:
                return "Stored [-1000]"
            elif self.value == -1001:
                return "Unset [-1001]"
        return str(self.value)

@nativedecorator
class Partition(object):
    """
    Represents a Kafka partition.
    """

    def __init__(self, **kwargs):
        """
        Initializes a new instance of Partition.
        """

        self._value = None

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = pi(net_pointer)
            return

        raise TypeError(f"TopicPartition does not support initialization")

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the partition instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the partition instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def value(self) -> int:
        """
        Gets the int value corresponding to this partition.

        Returns:
            int: The int value corresponding to this partition.
        """
        if self._value is None:
            self._value = self._interop.get_Value()

        return self._value

    @property
    def is_special(self) -> bool:
        return self.value == -1

    def __str__(self):
        if self.is_special:
            return '[Any]'
        return f'[{self.value}]'


@nativedecorator
class TopicPartition(object):
    """
    Represents a Kafka (topic, partition) tuple.
    """

    def __init__(self, **kwargs):
        """
        Initializes a new instance of TopicPartition.
        """

        self._topic = None
        self._partition = None
        self._as_string = None

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = tpi(net_pointer)
            return

        raise TypeError(f"TopicPartition does not support initialization")

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the TopicPartition instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the TopicPartition instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def topic(self) -> str:
        """
        Gets the Kafka topic name

        Returns:
            bytes: The Kafka topic name
        """
        if self._topic is None:
            self._topic = self._interop.get_Topic()
        return self._topic

    @property
    def partition(self) -> Partition:
        """
        Gets the Kafka partition

        Returns:
            bytes: The Kafka partition
        """
        if self._partition is None:
            self._partition = Partition(net_pointer=self._interop.get_Partition())
        return self._partition

    def __str__(self):
        if self._as_string is None:
            self._as_string = self._interop.ToString()
        return self._as_string

@nativedecorator
class TopicPartitionOffset(object):
    """
    Represents a Kafka (topic, partition, offset) tuple.
    """

    def __init__(self, **kwargs):
        """
        Initializes a new instance of TopicPartitionOffset.
        """

        self._topic = None
        self._topic_partition = None
        self._offset = None
        self._as_string = None

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = tpoi(net_pointer)
            return

        raise TypeError(f"TopicPartitionOffset does not support initialization")

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the TopicPartitionOffset instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the TopicPartitionOffset instance.
        """

        return self._interop.get_interop_ptr__()

    @property
    def topic(self) -> str:
        """
        Gets the Kafka topic name

        Returns:
            bytes: The Kafka topic name
        """
        if self._topic is None:
            self._topic = self._interop.get_Topic()
        return self._topic

    @property
    def topic_partition(self) -> TopicPartition:
        """
        Gets the Kafka (topic, partition) tuple

        Returns:
            Partition: The Kafka partition
        """
        if self._topic_partition is None:
            self._topic_partition = TopicPartition(net_pointer=self._interop.get_TopicPartition())
        return self._topic_partition

    @property
    def partition(self) -> Partition:
        """
        Gets the Kafka partition

        Returns:
            Partition: The Kafka partition
        """
        return self.topic_partition.partition

    @property
    def offset(self) -> Offset:
        """
        Gets the Kafka partition offset

        Returns:
            Partition: The Kafka partition offset
        """
        if self._offset is None:
            self._offset = Offset(net_pointer=self._interop.get_Offset())
        return self._offset

    def __str__(self):
        if self._as_string is None:
            self._as_string = self._interop.ToString()
        return self._as_string
