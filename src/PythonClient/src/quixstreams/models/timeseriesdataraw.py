import ctypes
import math
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .timeseriesdata import TimeseriesData
from ..helpers.nativedecorator import nativedecorator
from ..helpers.timeconverter import TimeConverter
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai, Array
from ..native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di, Dictionary
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from ..native.Python.QuixStreamsTelemetry.Models.TimeseriesDataRaw import TimeseriesDataRaw as tsdri
from ..native.Python.QuixStreamsStreaming.Models.TimeseriesData import TimeseriesData as tsdi


@nativedecorator
class TimeseriesDataRaw(object):
    """
    Describes timeseries data in a raw format for multiple timestamps. Class is intended for read only.
    """

    def __init__(self, net_pointer: ctypes.c_void_p = None):
        """
        Initializes a new instance of TimeseriesDataRaw.

        Args:
            net_pointer: Pointer to an instance of a .net TimeseriesDataRaw. Defaults to None.
        """


        if net_pointer is None:
            self._interop = tsdri(tsdri.Constructor())
        else:
            self._interop = tsdri(net_pointer)

        self._timestamps = None
        self._numeric_values = None
        self._string_values = None
        self._binary_values = None
        self._tag_values = None

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

    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts TimeseriesDataRaw to pandas DataFrame.

        Returns:
            pd.DataFrame: Converted pandas DataFrame.
        """


        if len(self.timestamps) == 0:
            return pd.DataFrame()

        headers = [
            "timestamp",
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

            # Add numerics
            for _, param_vals in self.numeric_values.items():
                row[i] = param_vals[rowindex]
                i = i + 1

            # Add strings_array
            for _, param_vals in self.string_values.items():
                row[i] = param_vals[rowindex]
                i = i + 1

            # Add binaries
            for _, param_vals in self.binary_values.items():
                row[i] = param_vals[rowindex]
                i = i + 1

            # add tags
            for _, tag_vals in self.tag_values.items():
                row[i] = tag_vals[rowindex]
                i = i + 1

            rows[rowindex] = row

        return pd.DataFrame(rows, columns=headers)

    @staticmethod
    def from_dataframe(data_frame: pd.DataFrame, epoch: int = 0) -> 'TimeseriesDataRaw':
        """
        Converts from pandas DataFrame to TimeseriesDataRaw.

        Args:
            data_frame: The pandas DataFrame to convert to TimeseriesData.
            epoch: The epoch to add to each time value when converting to TimeseriesData. Defaults to 0.

        Returns:
            TimeseriesDataRaw: Converted TimeseriesDataRaw.
        """

        if data_frame is None:
            return None

        parameter_data_raw = TimeseriesDataRaw()

        possible_time_labels = set(['time', 'timestamp', 'datetime'])

        # first or default in columns
        time_label = next((x for x in data_frame.columns if x.lower() in possible_time_labels), None)

        if time_label is None:
            possible_time_vals = data_frame.select_dtypes(include=['int', 'int64'])
            if possible_time_vals.count()[0] == 0:
                raise Exception(
                    "pandas DataFrame does not contain a suitable time column. Make sure to label the column 'time' or 'timestamp', else first integer column will be picked up as time")
            time_label = possible_time_vals.columns[0]

        def get_value_as_type(val_type, val, type_conv=None) -> List:
            if not isinstance(val, val_type) and type_conv is not None:
                new_val = type_conv(val)
                return new_val
            return val

        def convert_time(val):
            if isinstance(val, int):
                return val
            if isinstance(val, datetime):
                if isinstance(val, pd.Timestamp):
                    val = val.to_pydatetime()
                return TimeConverter.to_unix_nanoseconds(val)
            if isinstance(val, str):
                return TimeConverter.from_string(val)
            return int(val)

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

        rows_no = len(data_frame.index)  # dataframe rows count
        timestamps = [None] * rows_no
        string_values = {}
        binary_values = {}
        numeric_values = {}
        tag_values = {}

        def _add_tag(tag_colname: str, index: int, value: str):
            if tag_colname not in tag_values:
                tag_values[tag_colname] = [None] * rows_no
            tag_values[tag_colname][index] = value

        def _add_numeric(tag_colname: str, index: int, value: float):
            if tag_colname not in numeric_values:
                numeric_values[tag_colname] = [None] * rows_no
            numeric_values[tag_colname][index] = value

        def _add_string(tag_colname: str, index: int, value: str):
            if tag_colname not in string_values:
                string_values[tag_colname] = [None] * rows_no
            string_values[tag_colname][index] = value

        def _add_binary(tag_colname: str, index: int, value: bytes):
            if tag_colname not in binary_values:
                binary_values[tag_colname] = [None] * rows_no
            binary_values[tag_colname][index] = value

        row_index = -1
        for panda_index, panda_row in data_frame.iterrows():
            row_index = row_index + 1
            time = get_value_as_type(int, panda_row[time_label], convert_time)
            time = time - epoch
            timestamps[row_index] = time

            for panda_col_label, content in panda_row.items():
                panda_col_label = str(panda_col_label)  # just in case it is a numeric header
                if panda_col_label == time_label:
                    continue
                label_type = type(content)
                isnumeric = (label_type == int or label_type == float or label_type == complex)
                if isnumeric:
                    if math.isnan(content):
                        continue  # ignore it, as panda uses NaN instead of None, unable to detect difference
                    if panda_col_label.startswith('TAG__'):  # in case user of lib didn't put it in quote don't throw err
                        str_val = get_value_as_type(str, content, str_type_converter)
                        _add_tag(panda_col_label[5:], row_index, str_val)
                    else:
                        num_val = get_value_as_type(float, content, double_type_converter)
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
            timestamps: List[int],
            numeric_values: Dict[str, List[float]],
            string_values: Dict[str, List[str]],
            binary_values: Dict[str, List[bytes]],
            tag_values: Dict[str, List[str]]
    ):
        """
        Sets the values of the timeseries data from the provided dictionaries.
        Dictionary values are matched by index to the provided timestamps.

        Args:
            epoch: The time from which all timestamps are measured from.
            timestamps: The timestamps of values in nanoseconds since epoch as an array.
            numeric_values: The numeric values where the dictionary key is the parameter name and the value is the array of values.
            string_values: The string values where the dictionary key is the parameter name and the value is the array of values.
            binary_values: The binary values where the dictionary key is the parameter name and the value is the array of values.
            tag_values: The tag values where the dictionary key is the parameter name and the value is the array of values.
        """

        # set epoch
        self._interop.set_Epoch(epoch)

        # set timestamps
        self._timestamps = timestamps
        ts_uptr = Array.WriteLongs(timestamps)
        self._interop.set_Timestamps(ts_uptr)

        # set numeric values
        self._numeric_values = numeric_values
        nv_uptr = Dictionary.WriteStringNullableDoublesArray(numeric_values)
        self._interop.set_NumericValues(nv_uptr)

        # set string values
        self._string_values = string_values
        sv_uptr = Dictionary.WriteStringStringsArray(string_values)
        self._interop.set_StringValues(sv_uptr)

        # set byte values
        self._binary_values = binary_values
        bv_uptr = Dictionary.WriteStringBytesArray(binary_values)
        self._interop.set_BinaryValues(bv_uptr)

        # set tags values
        self._tag_values = tag_values
        tv_uptr = Dictionary.WriteStringStringsArray(tag_values)
        self._interop.set_TagValues(tv_uptr)

    @property
    def epoch(self) -> int:
        """
        The Unix epoch from which all other timestamps in this model are measured, in nanoseconds.

        Returns:
            int: The Unix epoch (01/01/1970) in nanoseconds.
        """

        return self._interop.get_Epoch()

    @property
    def timestamps(self) -> List[int]:
        """
        The timestamps of values in nanoseconds since the epoch.
        Timestamps are matched by index to numeric_values, string_values, binary_values, and tag_values.

        Returns:
            List[int]: A list of timestamps in nanoseconds since the epoch.
        """

        # Convert time values
        if self._timestamps is None:
            array_uptr = self._interop.get_Timestamps()
            self._timestamps = ai.ReadLongs(array_uptr)

        return self._timestamps

    @property
    def numeric_values(self) -> Dict[str, List[Optional[float]]]:
        """
        The numeric values for parameters.
        The key is the parameter ID the values belong to. The value is the numerical values of the parameter.
        Values are matched by index to timestamps.

        Returns:
            Dict[str, List[Optional[float]]]: A dictionary mapping parameter IDs to lists of numerical values.
        """

        # Convert numeric values
        if self._numeric_values is None:
            dic_hptr = self._interop.get_NumericValues()
            self._numeric_values = InteropUtils.invoke_and_free(dic_hptr, lambda x: di.ReadStringNullableDoublesArray(di.ReadAnyHPtrToUPtr(x)), default={})

        return self._numeric_values

    @property
    def string_values(self) -> Dict[str, List[str]]:
        """
        The string values for parameters.
        The key is the parameter ID the values belong to. The value is the string values of the parameter.
        Values are matched by index to timestamps.

        Returns:
            Dict[str, List[str]]: A dictionary mapping parameter IDs to lists of string values
        """

        # Convert string values
        if self._string_values is None:
            dic_hptr = self._interop.get_StringValues()
            self._string_values = InteropUtils.invoke_and_free(dic_hptr, lambda x: di.ReadStringStringsArray(di.ReadAnyHPtrToUPtr(x)), default={})

        return self._string_values

    @property
    def binary_values(self) -> Dict[str, List[bytes]]:
        """
        The binary values for parameters.
        The key is the parameter ID the values belong to.
        The value is the binary values of the parameter. Values are matched by index to timestamps.

        Returns:
            Dict[str, List[bytes]]: A dictionary mapping parameter IDs to lists of bytes values
        """

        # Convert binary values
        if self._binary_values is None:
            dic_hptr = self._interop.get_BinaryValues()
            self._binary_values = InteropUtils.invoke_and_free(dic_hptr, lambda x: di.ReadStringBytesArray(di.ReadAnyHPtrToUPtr(x)), default={})

        return self._binary_values

    @property
    def tag_values(self) -> Dict[str, List[str]]:
        """
        The tag values for parameters.
        The key is the parameter ID the values belong to. The value is the tag values of the parameter.
        Values are matched by index to timestamps.

        Returns:
            Dict[str, List[str]]: A dictionary mapping parameter IDs to lists of string values
        """

        # Convert tag values
        if self._tag_values is None:
            dic_hptr = self._interop.get_TagValues()
            self._tag_values = InteropUtils.invoke_and_free(dic_hptr, lambda x: di.ReadStringStringsArray(di.ReadAnyHPtrToUPtr(x)), default={})

        return self._tag_values

    def convert_to_timeseriesdata(self) -> TimeseriesData:
        """
        Converts TimeseriesDataRaw to TimeseriesData
        """
        ptr = tsdi.Constructor2(self.get_net_pointer())
        return TimeseriesData(ptr)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
