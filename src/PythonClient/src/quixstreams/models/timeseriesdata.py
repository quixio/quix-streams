import ctypes
import math
from datetime import datetime, timedelta
from typing import List, Optional
from typing import Union

import pandas as pd

from .parametervalue import ParameterValueType
from .timeseriesdatatimestamp import TimeseriesDataTimestamp
from ..helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ..helpers.nativedecorator import nativedecorator
from ..helpers.timeconverter import TimeConverter
from ..native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from ..native.Python.QuixStreamsStreaming.Models.TimeseriesData import TimeseriesData as tsdi


@nativedecorator
class TimeseriesData(object):
    """
    Describes timeseries data for multiple timestamps.
    """

    def __init__(self, net_pointer: ctypes.c_void_p = None):
        """
        Initializes a new instance of TimeseriesData.

        Args:
            net_pointer: Pointer to an instance of a .net TimeseriesData.
        """

        if net_pointer is None:
            self._interop = tsdi(tsdi.Constructor())
        else:
            self._interop = tsdi(net_pointer)

        self._timestamps = None
        self._init_timestamps()  # else the first time addition could result in double counting it in local cache

    def _finalizerfunc(self):
        if self._timestamps is not None:
            [pdstamp.dispose() for pdstamp in self._timestamps]
            self._timestamps = None


    # TODO
    # @classmethod
    # def from_timestamps(cls, timestamps: List[QuixStreams.Streaming.models.timeseriesdataTimestamp], merge: bool = True, clean: bool = True):
    #     """
    #         Creates a new instance of TimeseriesData with the provided timestamps rows.
    #
    #         :param timestamps: The timestamps with timeseries data.
    #         :param merge: Merge duplicated timestamps.
    #         :param clean: Clean timestamps without values.
    #
    #     """
    #
    #     dotnet_list = System.Collections.Generic.List[QuixStreams.Streaming.models.timeseriesdataTimestamp]([])
    #     for item in timestamps:
    #         dotnet_item = item.convert_to_net()
    #         dotnet_list.Add(dotnet_item)
    #
    #     self = cls()
    #
    #     self.__wrapped = QuixStreams.Streaming.models.timeseriesdata(dotnet_list, merge, clean)
    #
    #     return self

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

    def clone(self, parameter_filter: Optional[List[str]] = None):
        """
        Initializes a new instance of timeseries data with parameters matching the filter if one is provided.

        Args:
            parameter_filter: The parameter filter. If one is provided, only parameters
                                                   present in the list will be cloned.

        Returns:
            TimeseriesData: A new instance of TimeseriesData with filtered parameters.
        """
        new_net_instance = tsdi.Constructor2(self.get_net_pointer(), parameter_filter)
        return TimeseriesData(new_net_instance)

    def add_timestamp(self, time: Union[datetime, timedelta]) -> TimeseriesDataTimestamp:
        """
        Start adding a new set of parameters and their tags at the specified time.

        Args:
            time: The time to use for adding new event values.
                                               | datetime: The datetime to use for adding new event values. Epoch will never be added to this
                                               | timedelta: The time since the default epoch to add the event values at

        Returns:
            TimeseriesDataTimestamp: A new TimeseriesDataTimestamp instance.
        """
        if time is None:
            raise ValueError("'time' must not be None")
        if isinstance(time, datetime):
            netdate_hptr = dtc.datetime_to_dotnet(time)
            try:
                return self._add_to_timestamps(TimeseriesDataTimestamp(self._interop.AddTimestamp(netdate_hptr)))
            finally:
                InteropUtils.free_hptr(netdate_hptr)
        if isinstance(time, timedelta):
            nettimespan = dtc.timedelta_to_dotnet(time)
            return self._add_to_timestamps(TimeseriesDataTimestamp(self._interop.AddTimestamp2(nettimespan)))
        raise ValueError("'time' must be either datetime or timedelta")

    def add_timestamp_milliseconds(self, milliseconds: int) -> TimeseriesDataTimestamp:
        """
        Start adding a new set of parameters and their tags at the specified time.

        Args:
            milliseconds: The time in milliseconds since the default epoch to add the event values at.

        Returns:
            TimeseriesDataTimestamp: A new TimeseriesDataTimestamp instance.
        """

        return self._add_to_timestamps(TimeseriesDataTimestamp(self._interop.AddTimestampMilliseconds(milliseconds)))

    def add_timestamp_nanoseconds(self, nanoseconds: int) -> TimeseriesDataTimestamp:
        """
        Start adding a new set of parameters and their tags at the specified time.

        Args:
            nanoseconds: The time in nanoseconds since the default epoch to add the event values at.

        Returns:
            TimeseriesDataTimestamp: A new TimeseriesDataTimestamp instance.
        """

        return self._add_to_timestamps(TimeseriesDataTimestamp(self._interop.AddTimestampNanoseconds(nanoseconds)))

    def _add_to_timestamps(self, pdts):
        """
        Additional timestamps to keep track of if the timestamps were already initialized.
        """

        self.timestamps.append(pdts)
        return pdts

    @property
    def timestamps(self) -> List[TimeseriesDataTimestamp]:
        """
        Gets the data as rows of TimeseriesDataTimestamp.

        Returns:
            List[TimeseriesDataTimestamp]: A list of TimeseriesDataTimestamp instances.
        """

        return self._timestamps

    def _init_timestamps(self):

        if self._timestamps is not None:
            return

        def _converter_to_python(val: ctypes.c_void_p) -> TimeseriesDataTimestamp:
            if val is None:
                return None
            return TimeseriesDataTimestamp(val)

        timestamps_hptr = self._interop.get_Timestamps()
        refs = InteropUtils.invoke_and_free(timestamps_hptr, ei.ReadReferences)
        self._timestamps = [_converter_to_python(refs[i]) for i in range(len(refs))]

    @timestamps.setter
    def timestamps(self, timestamp_list: List[TimeseriesDataTimestamp]) -> None:
        """
        Sets the data as rows of TimeseriesDataTimestamp.

        Args:
            timestamp_list: A list of TimeseriesDataTimestamp instances to set.
        """

        raise NotImplemented("To be implemented in upcoming versions")
        dotnet_list = System.Collections.Generic.List[QuixStreams.Streaming.models.timeseriesdataTimestamp]([])
        for item in timestamp_list:
            dotnet_item = item.convert_to_net()
            dotnet_list.Add(dotnet_item)

        self.__wrapped.Timestamps = dotnet_list

    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts TimeseriesData to pandas DataFrame.

        Returns:
            pd.DataFrame: Converted pandas DataFrame.
        """

        def _build_headers(pdts):
            # object containing the index to the result array
            headers = {
                'timestamp': 0
            }
            default_row = [
                None
            ]

            max_index = 1

            for pdts in self.timestamps:
                for key, val in pdts.parameters.items():
                    if key not in headers:
                        headers[key] = max_index
                        headers[key] = max_index
                        max_index = max_index + 1
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

                for key, _ in pdts.tags.items():
                    k = "TAG__" + key
                    if k not in headers:
                        headers[k] = max_index
                        max_index = max_index + 1
                        default_row.append(None)
            return (headers, default_row)

        headers, default_row = _build_headers(self.timestamps)

        def _build_row(pdts):
            obj = default_row.copy()

            # time
            index = headers["timestamp"]
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
                index = headers["TAG__" + key]
                obj[index] = val

            return obj

        df = pd.DataFrame(
            map(_build_row, self.timestamps),
            columns=headers
        )

        return df

    @staticmethod
    def from_panda_dataframe(data_frame: pd.DataFrame, epoch: int = 0) -> 'TimeseriesData':
        """
        Converts pandas DataFrame to TimeseriesData.

        Args:
            data_frame: The pandas DataFrame to convert to TimeseriesData.
            epoch: The epoch to add to each time value when converting to TimeseriesData. Defaults to 0.

        Returns:
            TimeseriesData: Converted TimeseriesData instance.
        """

        if data_frame is None:
            return None

        parameter_data = TimeseriesData()

        possible_time_labels = set(['time', 'timestamp', 'datetime'])

        # first or default in columns
        time_label = next((x for x in data_frame.columns if x.lower() in possible_time_labels), None)

        if time_label is None:
            possible_time_vals = data_frame.select_dtypes(include=['int', 'int64'])
            if possible_time_vals.count()[0] == 0:
                raise Exception(
                    "panda data frame does not contain a suitable time column. Make sure to label the column 'time' or 'timestamp', else first integer column will be picked up as time")
            time_label = possible_time_vals.columns[0]

        def get_value_as_type(val_type, val, type_conv=None) -> List:
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
            if isinstance(val, float):
                return int(val)
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

        for row_index, panda_row in data_frame.iterrows():
            time = get_value_as_type(int, panda_row[time_label], convert_time)
            time = time + epoch
            data_row = parameter_data.add_timestamp_nanoseconds(time)
            for panda_col_label, content in panda_row.items():
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
                        num_val = get_value_as_type(float, content, double_type_converter)
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

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the .net pointer of the current instance.

        Returns:
            ctypes.c_void_p: The .net pointer of the current instance
        """
        return self._interop.get_interop_ptr__()
