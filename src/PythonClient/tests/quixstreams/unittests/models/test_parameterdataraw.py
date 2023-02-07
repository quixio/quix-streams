import unittest

import pandas
from src.quixstreams import ParameterDataRaw

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

from pandas.util.testing import assert_frame_equal

class ParameterDataRawTests(unittest.TestCase):

    def test_constructor_all_properties_work(self):
        pdr = ParameterDataRaw()

        ts = pdr.timestamps
        tags = pdr.tag_values
        nums = pdr.numeric_values
        strings = pdr.string_values
        bins = pdr.binary_values
        epoch = pdr.epoch

        # assert by no exception


    def test_convert_to_parameterdata(self):
        # arrange
        df = pandas.DataFrame([
            {"time": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number": 0.123, "string": "string value", "binary": bytes("binary", "utf-8")}
        ])

        # act
        raw = ParameterDataRaw.from_panda_frame(df)
        data = raw.convert_to_parameterdata()
        result = data.to_panda_frame()

        # assert
        assert_frame_equal(df.sort_index(axis=1), result.sort_index(axis=1), check_names=True)

    def test_from_panda_frame(self):
        # arrange
        df =  pandas.DataFrame([
            {"time": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number": 0.123, "string": "string value", "binary": bytes("binary", "utf-8")}
        ])

        # act
        raw = ParameterDataRaw.from_panda_frame(df)
        raw_ptr = raw.get_net_pointer()
        reloaded_from_csharp = ParameterDataRaw(raw_ptr)  # this will force us to load data back from the assigned values in c# rather than checking against potentially cached vals

        # assert
        self.assertEqual(reloaded_from_csharp.timestamps, [1000000])
        self.assertEqual(reloaded_from_csharp.tag_values, {'tag1': ['tag1_value'], 'tag2': ['tag2_value']})
        self.assertEqual(reloaded_from_csharp.string_values, {'string': ['string value']})
        self.assertEqual(reloaded_from_csharp.numeric_values, {'number': [0.123]})
        self.assertEqual(reloaded_from_csharp.binary_values, {'binary': [b'binary']})
        self.assertEqual(reloaded_from_csharp.epoch, 0)

    def test_from_panda_dataframe_equals_to_panda_frame(self):
        # test whether the from_panda_frame -> to_panda_frame equals
        def _assert_test(pdf):

            def assertFrameEqual(df1, df2, **kwds ):
                """ Assert that two dataframes are equal, ignoring ordering of columns"""
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