from quixstreaming.helpers import *
from .. import __importnet
from .parameterdata import ParameterData
from quixstreaming.helpers import *
import Quix.Sdk.Streaming
import Quix.Sdk.Process.Models
import clr
clr.AddReference('System.Collections')
import System
import math

from datetime import datetime, timedelta
from typing import Dict, List, Union

import pandas as pd


class ParameterDataRaw(object):
    """
    Describes parameter data in a raw format for multiple timestamps. Class is intended for read only.
    """
    def __init__(self, net_object: Quix.Sdk.Process.Models.ParameterDataRaw = None):
        """
            Initializes a new instance of ParameterDataRaw.

            :param net_object: Can be ignored. The underlying .net object representing ParameterData.
        """

        if net_object is None:
            net_object = Quix.Sdk.Process.Models.ParameterDataRaw()
            # raise Exception("ParameterDataRaw is none")

        self.__timestamps = None
        self.__numeric_values = None
        self.__string_values = None
        self.__binary_values = None
        self.__tag_values = None
        self.__wrapped = net_object

    def __str__(self):
        text = "  Length:" + str(len(self.timestamps))
        for index, val in enumerate(self.timestamps):
            text += "\r\n    Time:" + str(val)

            tags = {}
            for tag_id, tag_vals in self.tag_values.items():
                tag_val = tag_vals[index]
                if tag_val is not None:
                    tags[tag_id] = tag_val
            text += "\r\n      Tags: " + str(tags)

            parameter_values = {}
            for param_id, param_vals in self.numeric_values.items():
                param_val = param_vals[index]
                if param_val is not None:
                    parameter_values[param_id] = str(param_val)
            if len(parameter_values) > 0:
                for param_id, param_val in parameter_values.items():
                    text += "\r\n      " + param_id + ": " + param_val

            parameter_values = {}
            for param_id, param_vals in self.string_values.items():
                param_val = param_vals[index]
                if param_val is not None:
                    parameter_values[param_id] = str(param_val)
            if len(parameter_values) > 0:
                for param_id, param_val in parameter_values.items():
                    text += "\r\n      " + param_id + ": " + param_val

            parameter_values = {}
            for param_id, param_vals in self.binary_values.items():
                param_val = param_vals[index]
                if param_val is not None:
                    parameter_values[param_id] = "byte[" + str(len(param_val)) + "]"
            if len(parameter_values) > 0:
                for param_id, param_val in parameter_values.items():
                    text += "\r\n      " + param_id + ": " + param_val

        return text

    def to_panda_frame(self) -> pd.DataFrame:
        """
        Converts ParameterDataRaw to Panda DataFrame

        :return: Converted Panda DataFrame
        """

        headers = []
        if len(self.timestamps) > 0:
            headers = [
                "time", 
                *map(lambda x: x[0], self.numeric_values.items()), 
                *map(lambda x: x[0], self.string_values.items()), 
                *map(lambda x: x[0], self.binary_values.items()),                 
                *map(lambda x: f"TAG__{x[0]}", self.tag_values.items())
            ] 


        rows = [None] * len(self.timestamps)
        for rowindex, timestamp in enumerate(self.timestamps):
            row = [None] * len(headers)

            row[0] = timestamp

            i = 1

            #Add numerics
            for _, param_vals in self.numeric_values.items():
                row[i] = param_vals[rowindex]
                i = i+1

            #Add strings
            for _, param_vals in self.string_values.items():
                row[i] = param_vals[rowindex]
                i = i+1

            #Add binaries
            for _, param_vals in self.binary_values.items():
                row[i] = param_vals[rowindex]
                i = i+1

            #add tags
            for _, tag_vals in self.tag_values.items():
                row[i] = tag_vals[rowindex]
                i = i+1
            
            rows[rowindex] = row


        return pd.DataFrame(rows, columns=headers)

    def __str__(self):
        text = "ParameterDataRaw:"
        text += "\r\n  epoch: " + str(self.epoch)
        text += "\r\n            timestamps: " + str(self.timestamps)
        text += "\r\n        numeric_values: " + str(self.numeric_values)
        text += "\r\n         string_values: " + str(self.string_values)
        text += "\r\n         binary_values: " + str(self.binary_values)
        text += "\r\n            tag_values: " + str(self.tag_values)
        return text


    @staticmethod
    def from_panda_frame(data_frame: pd.DataFrame, epoch: int = 0) -> 'ParameterDataRaw':
        """
        Converts Panda DataFrame to ParameterData

        :param data_frame: The Panda DataFrame to convert to ParameterData
        :param epoch: The epoch to add to each time value when converting to ParameterData. Defaults to 0
        :return: Converted ParameterData
        """

        if data_frame is None:
            return None

        parameter_data_raw = ParameterDataRaw()

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

        rows_no = len(data_frame.index) #dataframe rows count
        timestamps = [None] * rows_no
        string_values = {}
        binary_values = {}
        numeric_values = {}
        tag_values = {}


        def _add_tag(tag_colname:str, index:int, value:str):
            if tag_colname not in tag_values:
                tag_values[tag_colname] = [None] * rows_no
            tag_values[tag_colname][index] = value
        def _add_numeric(tag_colname:str, index:int, value: float):
            if tag_colname not in numeric_values:
                numeric_values[tag_colname] = [None] * rows_no
            numeric_values[tag_colname][index] = value
        def _add_string(tag_colname:str, index:int, value: str):
            if tag_colname not in string_values:
                string_values[tag_colname] = [None] * rows_no
            string_values[tag_colname][index] = value
        def _add_binary(tag_colname:str, index:int, value:bytes):
            if tag_colname not in binary_values:
                binary_values[tag_colname] = [None] * rows_no
            binary_values[tag_colname][index] = value

        row_index = -1
        for panda_index, panda_row in data_frame.iterrows():
            row_index = row_index + 1
            time = get_value_as_type(System.Int64, panda_row[time_label], convert_time)
            time = time - epoch
            timestamps[row_index] = time

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
                        _add_tag(panda_col_label[5:], row_index, str_val)
                    else:
                        num_val = get_value_as_type(nullable_double_type, content, double_type_converter)
                        _add_numeric(panda_col_label, row_index, num_val)
                else:
                    isbytes = (label_type == bytes or label_type == bytearray)
                    if isbytes:
                        if panda_col_label.startswith('TAG__'):
                            str_val = get_value_as_type(str, content, str_type_converter)
                            _add_tag(panda_col_label[5:], row_index, str_val)
                        else:
                            bytes_val = get_value_as_type(bytes, content, bytes_type_converter)
                            _add_binary(panda_col_label, row_index, bytes_val)
                    else:
                        str_val = get_value_as_type(str, content, str_type_converter)
                        if panda_col_label.startswith('TAG__'):
                            _add_tag(panda_col_label[5:], row_index, str_val)
                        else:
                            _add_string(panda_col_label, row_index, str_val)

        parameter_data_raw.set_values(
            epoch=epoch, 
            timestamps=timestamps,
            numeric_values=numeric_values,
            string_values=string_values,
            tag_values=tag_values,
            binary_values=binary_values
        )

        return parameter_data_raw

    def set_values(
            self, 
            epoch: int, 
            timestamps:[int], 
            numeric_values: Dict[str, List[float]],
            string_values: Dict[str, List[str]],
            tag_values: Dict[str, List[str]],
            binary_values: Dict[str, List[bytes]]
        ):
        """
            The timestamps of values in nanoseconds since epoch.
            Timestamps are matched by index to numeric_values, string_values, binary_values and tag_values
        """

        def _map_dictionary(dct, type, mapper=None):
            d = System.Collections.Generic.Dictionary[System.String, type]({})
            for key in dct:
                d[key] = mapper(dct[key])
            return d

        nullable_double_type = type(System.Nullable[System.Double](0))

        #set epoch
        self.__wrapped.Epoch = System.Int64(epoch)
        
        #set timestamps
        self.__timestamps = timestamps
        self.__wrapped.Timestamps = System.Array[System.Int64](self.__timestamps)

        #set numeric values
        self.__numeric_values = numeric_values
        self.__wrapped.NumericValues = _map_dictionary(numeric_values, System.Array[nullable_double_type], lambda x: System.Array[nullable_double_type](x))

        #set string values
        self.__string_values = string_values
        self.__wrapped.StringValues = _map_dictionary(string_values, System.Array[System.String], lambda x: System.Array[System.String](x))

        #set byte values
        self.__binary_values = binary_values
        self.__wrapped.BinaryValues = _map_dictionary(binary_values, System.Array[System.Array[System.Byte]], lambda x: System.Array[System.Array[System.Byte]](x))

        #set tags values
        self.__tag_values = tag_values
        self.__wrapped.TagValues = _map_dictionary(tag_values, System.Array[System.String], lambda x: System.Array[System.String](x))

        pass

    @property
    def epoch(self) -> int:
        """
            The unix epoch from, which all other timestamps in this model are measured from in nanoseconds.
            0 = UNIX epoch (01/01/1970)
        """

        return self.__wrapped.Epoch

    @property
    def timestamps(self) -> [int]:
        """
            The timestamps of values in nanoseconds since epoch.
            Timestamps are matched by index to numeric_values, string_values, binary_values and tag_values
        """

        # Convert time values
        if self.__timestamps is None:
            self.__timestamps = NetToPythonConverter.convert_array(self.__wrapped.Timestamps, lambda x:x)

        return self.__timestamps

    @property
    def numeric_values(self) -> Dict[str, List[float]]:
        """
            The numeric values for parameters.
            The key is the parameter Id the values belong to
            The value is the numerical values of the parameter. Values are matched by index to timestamps.
        """

        # Convert numeric values
        if self.__numeric_values is None:
            def convertRow(row):
                if row is None:
                    return row
                items = []
                for i in row:
                    items.append(i)
                return items
            self.__numeric_values = NetToPythonConverter.convert_dict(self.__wrapped.NumericValues, convertRow)

        return self.__numeric_values

    @property
    def string_values(self) -> Dict[str, List[str]]:
        """
            The string values for parameters.
            The key is the parameter Id the values belong to
            The value is the string values of the parameter. Values are matched by index to timestamps.
        """

        # Convert string values
        if self.__string_values is None:
            def convertRow(row):
                if row is None:
                    return row
                items = []
                for i in row:
                    items.append(i)
                return items
            self.__string_values = NetToPythonConverter.convert_dict(self.__wrapped.StringValues, convertRow)

        return self.__string_values

    @property
    def binary_values(self) -> Dict[str, List[bytes]]:
        """
            The binary values for parameters.
            The key is the parameter Id the values belong to
            The value is the binary values of the parameter. Values are matched by index to timestamps
        """

        # Convert binary values
        if self.__binary_values is None:
            def translateNothing(x):
                if x is not None and isinstance(x, System.Array[System.Byte]):
                    return bytes(x)
                return x
            def convertRow(row):
                if row is None:
                    return row
                return NetToPythonConverter.convert_list(row, translateNothing)
            self.__binary_values = NetToPythonConverter.convert_dict(self.__wrapped.BinaryValues, convertRow)

        return self.__binary_values

    @property
    def tag_values(self) -> Dict[str, List[str]]:
        """
            The tag values for parameters.
            The key is the parameter Id the values belong to
            The value is the tag values of the parameter. Values are matched by index to timestamps
        """

        # Convert tag values
        if self.__tag_values is None:
            def translateNothing(x):
                return x
            def convertRow(row):
                if row is None:
                    return row
                return NetToPythonConverter.convert_list(row, translateNothing)
            self.__tag_values = NetToPythonConverter.convert_dict(self.__wrapped.TagValues, convertRow)

        return self.__tag_values

    def convert_to_parameterdata(self) -> ParameterData:
        return ParameterData(Quix.Sdk.Streaming.Models.ParameterData(self.__wrapped))

    def convert_to_net(self):
        return self.__wrapped
