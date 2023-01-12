import math

from quixstreaming.models.netlist import NetReadonlyList

from datetime import datetime, timedelta
from quixstreaming.helpers import *
from typing import Union

from .parameterdatatimestamp import ParameterDataTimestamp
from .parametervalue import ParameterValueType
from .. import __importnet
import Quix.Sdk.Streaming
import clr
clr.AddReference('System.Collections')
import System

from typing import Dict, List
import pandas as pd


class ParameterData(object):
    """
    Describes parameter data for multiple timestamps
    """
    def __init__(self, net_object: Quix.Sdk.Streaming.Models.ParameterData = None):
        """
            Initializes a new instance of ParameterData.

            :param net_object: Can be ignored. The underlying .net object representing ParameterData.
        """

        if net_object is None:
            net_object = Quix.Sdk.Streaming.Models.ParameterData()
        self.__wrapped = net_object

    @classmethod
    def from_timestamps(cls, timestamps: List[Quix.Sdk.Streaming.Models.ParameterDataTimestamp], merge: bool = True, clean: bool = True):
        """
            Creates a new instance of ParameterData with the provided timestamps rows.

            :param timestamps: The timestamps with parameter data.
            :param merge: Merge duplicated timestamps.
            :param clean: Clean timestamps without values.

        """

        dotnet_list = System.Collections.Generic.List[Quix.Sdk.Streaming.Models.ParameterDataTimestamp]([])
        for item in timestamps:
            dotnet_item = item.convert_to_net()
            dotnet_list.Add(dotnet_item)

        self = cls()

        self.__wrapped = Quix.Sdk.Streaming.Models.ParameterData(dotnet_list, merge, clean)

        return self

    def __str__(self):
        text = "  Length:" + str(len(self.timestamps))
        for index, val in enumerate(self.timestamps):
            text += "\r\n    Time:" + str(val.timestamp_nanoseconds)
            text += "\r\n      Tags: " + str(val.tags)
            text += "\r\n      Params:"
            for param_id, param_val in val.parameters.items():
                if param_val.type == ParameterValueType.Numeric:
                    text += "\r\n        " + str(param_id) + ": " + str(param_val.numeric_value)
                    continue
                if param_val.type == ParameterValueType.String:
                    text += "\r\n        " + str(param_id) + ": " + str(param_val.string_value)
                    continue
                if param_val.type == ParameterValueType.Binary:
                    val = param_val.binary_value
                    if val is None:
                        val = b''
                    text += "\r\n        " + str(param_id) + ": byte[" + str(len(val)) + "]"
                    continue
                if param_val.type == ParameterValueType.Empty:
                    text += "\r\n        " + str(param_id) + ": Empty"
                    continue
                text += "\r\n        " + str(param_id) + ": ???"
        return text

    def clone(self, parameter_filter: [str] = None):
        """
            Initializes a new instance of parameter data with parameters matching the filter if one is provided

            Parameters:

            :param parameter_filter: The parameter filter. If one is provided, only parameters present in the list will be cloned
        """
        new_net_instance = Quix.Sdk.Streaming.Models.ParameterData(self.__wrapped, parameter_filter)
        return ParameterData(new_net_instance)

    def add_timestamp(self, time: Union[datetime, timedelta]) -> ParameterDataTimestamp:
        """
        Start adding a new set parameters and their tags at the specified time
        :param time: The time to use for adding new event values.
                     | datetime: The datetime to use for adding new event values. Epoch will never be added to this
                     | timedelta: The time since the default epoch to add the event values at

        :return: ParameterDataTimestamp
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            netdate = PythonToNetConverter.convert_datetime(time)
            return ParameterDataTimestamp(self.__wrapped.AddTimestamp(netdate))
        if isinstance(time, timedelta):
            nettimespan = PythonToNetConverter.convert_timedelta(time)
            return ParameterDataTimestamp(self.__wrapped.AddTimestamp(nettimespan))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_milliseconds(self, milliseconds: int) -> ParameterDataTimestamp:
        """
        Start adding a new set parameters and their tags at the specified time
        :param milliseconds: The time in milliseconds since the default epoch to add the event values at
        :return: ParameterDataTimestamp
        """
        return ParameterDataTimestamp(self.__wrapped.AddTimestampMilliseconds(milliseconds))

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> ParameterDataTimestamp:
        """
        Start adding a new set parameters and their tags at the specified time
        :param nanoseconds: The time in nanoseconds since the default epoch to add the event values at
        :return: ParameterDataTimestamp
        """
        return ParameterDataTimestamp(self.__wrapped.AddTimestampNanoseconds(nanoseconds))

    @property
    def timestamps(self) -> List[ParameterDataTimestamp]:
        """
            Gets the data as rows of ParameterDataTimestamp
        """

        def _converter_from_python(val: ParameterDataTimestamp):
            if val is None:
                return None
            return val.convert_to_net()

        def _converter_to_python(val: Quix.Sdk.Streaming.Models.ParameterDataTimestamp):
            if val is None:
                return None
            return ParameterDataTimestamp(val)

        return NetReadonlyList(self.__wrapped.Timestamps, converter_from_python=_converter_from_python,
                       converter_to_python=_converter_to_python)

    @timestamps.setter
    def timestamps(self, timestamp_list: List[ParameterDataTimestamp]) -> None:
        """
            Sets the data as rows of ParameterDataTimestamp
        """

        dotnet_list = System.Collections.Generic.List[Quix.Sdk.Streaming.Models.ParameterDataTimestamp]([])
        for item in timestamp_list:
            dotnet_item = item.convert_to_net()
            dotnet_list.Add(dotnet_item)

        self.__wrapped.Timestamps = dotnet_list

    def to_panda_frame(self) -> pd.DataFrame:
        """
        Converts ParameterData to Panda DataFrame

        :return: Converted Panda DataFrame
        """

            
        def _build_headers(pdts):
            #object containing the index to the result array
            headers = {
                'time': 0
            }
            default_row = [
                None
            ]

            nan = float('nan')

            max_index = 1
            for pdts in self.timestamps:
                for key,val in pdts.parameters.items():
                    if key not in headers:
                        headers[key] = max_index
                        headers[key] = max_index
                        max_index = max_index+1
                        if val.type == ParameterValueType.Numeric:
                            default_row.append(None)
                        elif val.type == ParameterValueType.String:
                            default_row.append(None)
                        elif val.type == ParameterValueType.Binary:
                            default_row.append(None)
                        elif val.type == ParameterValueType.Empty:
                            default_row.append(None)
                        else:
                            raise Exception("Unreachable")

                for key,_ in pdts.tags.items():
                    k = "TAG__"+key
                    if k not in headers:
                        headers[k] = max_index
                        max_index = max_index+1
                        default_row.append(None)
            return (headers, default_row)

        headers, default_row = _build_headers(self.timestamps)

        def _build_row(pdts):
            obj = default_row.copy()
            
            #time
            index = headers["time"]
            obj[index] = pdts.timestamp_nanoseconds

            for key, val in pdts.parameters.items():
                index = headers[key]
                if val.type == ParameterValueType.Numeric:
                    obj[index] = val.numeric_value
                    continue
                if val.type == ParameterValueType.String:
                    obj[index] = val.string_value
                    continue
                if val.type == ParameterValueType.Binary:
                    obj[index] = bytes(val.binary_value)

            for key, val in pdts.tags.items():
                index = headers["TAG__"+key]
                obj[index] = val

            return obj

        df = pd.DataFrame(
                map(_build_row, self.timestamps), 
                columns=headers
            )

        return df

    @staticmethod
    def from_panda_frame(data_frame: pd.DataFrame, epoch: int = 0) -> 'ParameterData':
        """
        Converts Panda DataFrame to ParameterData

        :param data_frame: The Panda DataFrame to convert to ParameterData
        :param epoch: The epoch to add to each time value when converting to ParameterData. Defaults to 0
        :return: Converted ParameterData
        """

        if data_frame is None:
            return None

        parameter_data = ParameterData()


        possible_time_labels = set(['time', 'timestamp', 'datetime'])

        #first or default in columns
        time_label = next((x for x in data_frame.columns if x.lower() in possible_time_labels), None)

        if time_label is None:
                possible_time_vals = data_frame.select_dtypes(include=['int', 'int64'])
                if possible_time_vals.count()[0] == 0:
                    raise Exception("Panda frame does not contain a suitable time column. Make sure to label the column 'time' or 'timestamp', else first integer column will be picked up as time")
                time_label = possible_time_vals.columns[0]

        def get_value_as_type(val_type, val, type_conv=None) -> []:
            if not isinstance(val, val_type) and type_conv is not None:
                new_val = type_conv(val)
                return new_val
            return val

        def convert_time(val):
            if isinstance(val, datetime):
                if isinstance(val, pd.Timestamp):
                    val = val.to_pydatetime()
                return TimeConverter.to_unix_nanoseconds(val)
            if isinstance(val, str):
                return TimeConverter.from_string(val)
            return val

        def str_type_converter(val_to_convert):
            if val_to_convert is None:
                return None
            return str(val_to_convert)

        def bytes_type_converter(val_to_convert):
            if val_to_convert is None:
                return None
            return bytes(val_to_convert)

        def double_type_converter(val_to_convert):
            if val_to_convert is None:
                return None
            if pd.isnull(val_to_convert):
                return None
            return float(val_to_convert)

        nullable_double_type = type(System.Nullable[System.Double](0))

        nan = float('nan')
        for row_index, panda_row in data_frame.iterrows():
            time = get_value_as_type(System.Int64, panda_row[time_label], convert_time)
            time = time + epoch
            data_row = parameter_data.add_timestamp_nanoseconds(time)
            for panda_col_label, content in panda_row.iteritems():
                panda_col_label = str(panda_col_label)  # just in case it is a numeric header
                if panda_col_label == time_label:
                    continue
                label_type = type(content)
                isnumeric = (label_type == int or label_type == float or label_type == complex)
                if isnumeric:
                    if math.isnan(content):
                        continue  # ignore it, as panda uses NaN instead of None, unable to detect difference
                    if panda_col_label.startswith('TAG__'):  # in case user of lib didnt put it in quote dont throw err
                        str_val = get_value_as_type(str, content, str_type_converter)
                        data_row.add_tag(panda_col_label[5:], str_val)
                    else:
                        num_val = get_value_as_type(nullable_double_type, content, double_type_converter)
                        data_row.add_value(panda_col_label, num_val)
                else:
                    isbytes = (label_type == bytes or label_type == bytearray)
                    if isbytes:
                        if panda_col_label.startswith('TAG__'):
                            str_val = get_value_as_type(str, content, str_type_converter)
                            data_row.add_tag(panda_col_label[5:], str_val)
                        else:
                            bytes_val = get_value_as_type(bytes, content, bytes_type_converter)
                            data_row.add_value(panda_col_label, bytes_val)
                    else:
                        str_val = get_value_as_type(str, content, str_type_converter)
                        if panda_col_label.startswith('TAG__'):
                            data_row.add_tag(panda_col_label[5:], str_val)
                        else:
                            data_row.add_value(panda_col_label, str_val)

        return parameter_data

    def convert_to_net(self):
        return self.__wrapped
