import ctypes
import datetime

from ...native.Python.SystemPrivateCoreLib.System.DateTime import DateTime as dti
from ...native.Python.SystemPrivateCoreLib.System.TimeSpan import TimeSpan as tsi
from ...native.Python.SystemPrivateCoreLib.System.DateTimeKind import DateTimeKind as DateTimeKindInterop



class DateTimeConverter:

    @staticmethod
    def datetime_to_python(hptr: ctypes.c_void_p) -> datetime.datetime:
        """
        Converts dotnet pointer to DateTime and frees the pointer.

        Args:
            hptr: Handler Pointer to .Net type DateTime

        Returns:
            datetime.datetime:
                Python type datetime
        """
        if hptr is None:
            return None

        with (dt := dti(hptr)):
            ticks = dt.get_Ticks()
            # due to precision loss when converting to float during division,
            # it is better to remove the ticks describing microsecond and add in another operation
            micro_ticks = int(ticks % 10000000)
            ticks_nomicro = ticks - micro_ticks
            parsed_nomicro = datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=ticks_nomicro / 10)
            parsed_withmicro = parsed_nomicro + datetime.timedelta(microseconds=micro_ticks / 10)
            return parsed_withmicro

    @staticmethod
    def datetime_to_dotnet(value: datetime.datetime) -> ctypes.c_void_p:
        """
        Args:
            value: Python type datetime

        Returns:
            ctypes.c_void_p:
                Handler Pointer to .Net type DateTime
        """
        if value is None:
            return None

        orig_value = value
        datetime_kind = DateTimeKindInterop.Local
        if orig_value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)
            datetime_kind = DateTimeKindInterop.Utc

        ms = int((value.microsecond - value.microsecond % 1000) / 1000)
        result_hptr = dti.Constructor15(value.year,
                                        value.month,
                                        value.day,
                                        value.hour,
                                        value.minute,
                                        value.second,
                                        ms,
                                        value.microsecond % 1000,
                                        datetime_kind)
        return result_hptr

    @staticmethod
    def timespan_to_python(uptr: ctypes.c_void_p) -> datetime.timedelta:
        """
        Converts dotnet pointer to Timespan as binary and frees the pointer.

        Args:
            uptr: Pointer to .Net type TimeSpan

        Returns:
            datetime.timedelta:
                Python type timedelta
        """
        if uptr is None:
            return None

        with (ts := tsi(uptr, finalize=False)):  # get_Ticks already disposes
            ticks = ts.get_Ticks()
            # due to precision loss when converting to float during division,
            # it is better to remove the ticks describing microsecond and add in another operation
            micro_ticks = int(ticks % 10000000)
            ticks_nomicro = ticks - micro_ticks
            parsed_nomicro = datetime.timedelta(microseconds=ticks_nomicro / 10)
            parsed_withmicro = parsed_nomicro + datetime.timedelta(microseconds=micro_ticks / 10)
            return parsed_withmicro

    @staticmethod
    def timedelta_to_dotnet(value: datetime.timedelta) -> ctypes.c_void_p:
        """
        Args:
            value: Python type timedelta

        Returns:
            ctypes.c_void_p:
                Pointer to unmanaged memory containing TimeSpan
        """

        if value is None:
            return None

        # TODO might need to use another constructor to avoid precision loss
        dotnet_uptr = tsi.Constructor(int(value.total_seconds() * 10000000))
        return dotnet_uptr
