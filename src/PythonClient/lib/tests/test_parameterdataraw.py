import unittest

import math
from datetime import datetime, timezone, timedelta
import tzlocal, pytz

import pandas
from quixstreaming.helpers.timeconverter import TimeConverter
from quixstreaming import ParameterDataRaw

from quixstreaming.models.parametervalue import ParameterValueType


class ParameterDataTests(unittest.TestCase):

    def test_from_panda_dataframe_equals_to_panda_frame(self):
        # test whether the from_panda_frame -> to_panda_frame equals
        def _assert_test(pdf):

            def assertFrameEqual(df1, df2, **kwds ):
                """ Assert that two dataframes are equal, ignoring ordering of columns"""
                from pandas.util.testing import assert_frame_equal
                df1 = df1.reset_index(drop=True)
                df2 = df2.reset_index(drop=True)
                assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds )

            parameter_data_raw = ParameterDataRaw.from_panda_frame(pdf)
            assertFrameEqual(
                        pdf,
                        parameter_data_raw.to_panda_frame()
                    )


        _assert_test(pandas.DataFrame([{"time": 1000000}]))
        _assert_test(pandas.DataFrame([{"time": 1000000}, {"time": 2000000}, {"time": 3000000}]))


        _assert_test(pandas.DataFrame([
            {"time": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number1":0.123, "number2":0.123}
        ]))
        _assert_test(pandas.DataFrame([
            {"time": 1000000, "TAG__tag1": "tag1_value_1", "TAG__tag2": "tag2_value_1", "number1":0.123, "number2":0.123},
            {"time": 2000000, "TAG__tag1": "tag1_value_2", "TAG__tag2": "tag2_value_2", "number1":0.623, "number2":0.1312}
        ]))

        _assert_test(pandas.DataFrame([
            {"time": 1000000, "TAG__tag1": "tag1_value_1", "TAG__tag2": "tag2_value_1", "number1": 0.123,
             "number2": 0.123},
            {"time": 2000000, "TAG__tag1": "tag1_value_2", "TAG__tag2": "tag2_value_2", "number1": 0.623,
             "number2": 0.1312}
        ]).head(2).tail(1))

    def test_multiple_time_columns(self):
        def _assert_time(pdf, time):
            parameter_data_raw = ParameterDataRaw.from_panda_frame(pdf).to_panda_frame()
            parsed_time=parameter_data_raw.loc[0, 'time']
            self.assertEqual(time, parsed_time)

        _assert_time(pandas.DataFrame([{"value": 0.1,"time": 1000000}]), 1000000)
        _assert_time(pandas.DataFrame([{"TiMe": 2000000, "value": 0.1}]), 2000000)
        _assert_time(pandas.DataFrame([{"value": 0.1,"datetime": 3000000}]), 3000000)
        _assert_time(pandas.DataFrame([{"TiMeSTAMP": 5000000, "value": 0.1}]), 5000000)
        _assert_time(pandas.DataFrame([{"value": 0.1, "TiMeSTAMP": 5000000}]), 5000000)


if __name__ == '__main__':
    unittest.main()
