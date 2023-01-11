from typing import Union, Dict

from quixstreaming.helpers import *
from quixstreaming.models.netdict import ReadOnlyNetDict
from datetime import datetime, timedelta

from quixstreaming.models.parametervalue import ParameterValue, ParameterValueType
from .. import __importnet
import Quix.Sdk.Streaming
import clr
clr.AddReference('System.Collections')
import System.Collections.Generic


class ParameterDataTimestamp(object):
    """
    Represents a single point in time with parameter values and tags attached to that time
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.ParameterDataTimestamp):
        """
            Initializes a new instance of ParameterDataTimestamp.

            Parameters:

            net_object (.net object): The .net object representing ParameterDataTimestamp.
        """
        self.__wrapped = net_object

    def __str__(self):
        text = "Time:" + str(self.timestamp_nanoseconds)
        text += "\r\n  Tags: " + str(self.tags)
        text += "\r\n  Params:"
        for param_id, param_val in self.parameters.items():
            if param_val.type == ParameterValueType.Numeric:
                text += "\r\n    " + str(param_id) + ": " + str(param_val.numeric_value)
                continue
            if param_val.type == ParameterValueType.String:
                text += "\r\n    " + str(param_id) + ": " + str(param_val.string_value)
                continue
            if param_val.type == ParameterValueType.Binary:
                text += "\r\n    " + str(param_id) + ": byte[" + str(len(param_val.binary_value)) + "]"
                continue
            if param_val.type == ParameterValueType.Empty:
                text += "\r\n    " + str(param_id) + ": Empty"
                continue
            text += "\r\n    " + str(param_id) + ": ???"
        return text

    @property
    def parameters(self) -> Dict[str, ParameterValue]:
        """
        Parameter values for the timestamp. When a key is not found, returns empty ParameterValue
        The dictionary key is the parameter id
        The dictionary value is the value (ParameterValue)
        """

        def _value_converter_to_python(net_object: Quix.Sdk.Streaming.Models.ParameterValue):
            if net_object is None:
                return None
            return ParameterValue(net_object)

        def _value_converter_from_python(param_val: ParameterValue):
            if param_val is None:
                return None
            return param_val.convert_to_net()

        return ReadOnlyNetDict(self.__wrapped.Parameters, val_converter_from_python=_value_converter_from_python,
                               val_converter_to_python=_value_converter_to_python)

    @property
    def tags(self) -> Dict[str, str]:
        """
        Tags for the timestamp. When key is not found, returns None
        The dictionary key is the tag id
        The dictionary value is the tag value
        """

        return ReadOnlyNetDict(self.__wrapped.Tags)

    @property
    def timestamp_nanoseconds(self) -> int:
        """Gets timestamp in nanoseconds"""

        return self.__wrapped.TimestampNanoseconds

    @property
    def timestamp_milliseconds(self) -> int:
        """Gets timestamp in milliseconds"""

        return self.__wrapped.TimestampMilliseconds

    @property
    def timestamp(self) -> datetime:
        """Gets the timestamp in datetime format"""
        return NetToPythonConverter.convert_datetime(self.__wrapped.Timestamp)

    @property
    def timestamp_as_time_span(self) -> timedelta:
        """Gets the timestamp in timespan format"""
        return NetToPythonConverter.convert_timespan(self.__wrapped.TimestampAsTimeSpan)

    def add_value(self, parameter_id: str, value: Union[float, str, int, bytearray, bytes]) -> 'ParameterDataTimestamp':
        """
            Adds a new value for the parameter
            :param parameter_id: The parameter to add the value for
            :param value: the value to add. Can be float or string

        :return: ParameterDataTimestamp
        """

        if type(value) is int:
            value = float(value)

        self.__wrapped.AddValue(parameter_id, value)
        return self

    def remove_value(self, parameter_id: str)  -> 'ParameterDataTimestamp':
        """
            Removes the value for the parameter
            :param parameter_id: The parameter to remove the value for

        :return: ParameterDataTimestamp
        """
        self.__wrapped.RemoveValue(parameter_id)
        return self

    def add_tag(self, tag_id: str, tag_value: str)  -> 'ParameterDataTimestamp':
        """
            Adds a tag to the values
            :param tag_id: The id of the tag to the set the value for
            :param tag_value: the value to set

        :return: ParameterDataTimestamp
        """
        self.__wrapped.AddTag(tag_id, tag_value)
        return self

    def remove_tag(self, tag_id: str)  -> 'ParameterDataTimestamp':
        """
            Removes a tag from the values
            :param tag_id: The id of the tag to remove

        :return: ParameterDataTimestamp
        """
        self.__wrapped.RemoveTag(tag_id)
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'ParameterDataTimestamp':
        """
            Copies the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: ParameterDataTimestamp
        """

        if tags is None:
            return self

        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def convert_to_net(self):
        return self.__wrapped
