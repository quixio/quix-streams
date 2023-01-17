from typing import Union, Dict
from quixstreaming import __importnet
import Quix.Sdk.Streaming
import clr
clr.AddReference('System.Collections')
import System.Collections.Generic


class ParameterDataBuilder(object):
    """
        Builder for creating parameter data packages for StreamPropertiesWriter
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamWriter.ParameterDataBuilder):
        """
            Initializes a new instance of ParameterDataBuilder.

            Parameters:

            net_object (.net object): The .net object representing ParameterDataBuilder.
        """

        self.__wrapped = net_object

    def add_value(self, parameter_id: str, value: Union[str, float, int]) -> 'ParameterDataBuilder':
        """
        Adds new parameter value at the time the builder is created for
        :param parameter_id: The id of the parameter to set the value for
        :param value: the value as string or float
        :return: The builder
        """

        if type(value) is int:
            value = float(value)

        self.__wrapped = self.__wrapped.AddValue(parameter_id, value)
        return self

    def add_tag(self, tag_id: str, value: str) -> 'ParameterDataBuilder':
        """
        Adds tag value for the values. If
        :param tag_id: The id of the tag
        :param value: The value of the tag
        :return: The builder
        """
        self.__wrapped = self.__wrapped.AddTag(tag_id, value)
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventDataBuilder':
        """
            Copies the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: EventDataBuilder
        """

        if tags is None:
            return self

        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def write(self):
        """
        Writes the values to the StreamParametersWriter buffer. See StreamParametersWriter buffer settings for more information when the values are sent to the broker
        """
        self.__wrapped.Write()
