import unittest

import pandas
from pandas.testing import assert_frame_equal

from src.quixstreams import TimeseriesDataRaw


class TimeseriesDataRawTests(unittest.TestCase):

    def test_constructor_all_properties_work(self):
        pdr = TimeseriesDataRaw()

        ts = pdr.timestamps
        tags = pdr.tag_values
        nums = pdr.numeric_values
        strings = pdr.string_values
        bins = pdr.binary_values
        epoch = pdr.epoch

        # assert by no exception


    def test_set_values(self):
        # act
        timeseries_data = TimeseriesDataRaw()
        timeseries_data.set_values(
            epoch=0,
            timestamps=[1, 2, 3],
            numeric_values={"numeric": [3, 12.32, None]},
            string_values={"string": ["one", None, "three"]},
            binary_values={"binary": [bytes("byte1", "utf-8"), None, None]},
            tag_values={"tag1": ["t1", "t2", None]},
        )

        # assert
        expected_df = pandas.DataFrame([
            {"timestamp": 1, "TAG__tag1": "t1", "numeric": 3, "string": "one", "binary": bytes("byte1", "utf-8")},
            {"timestamp": 2, "TAG__tag1": "t2", "numeric": 12.32},
            {"timestamp": 3, "string": "three"}
        ])

        df_orig = timeseries_data.to_dataframe()
        assert_frame_equal(expected_df.sort_index(axis=1), df_orig.sort_index(axis=1), check_names=True)

    def test_set_values_only_string(self):
        # act
        timeseries_data = TimeseriesDataRaw()
        timeseries_data.set_values(
            epoch=0,
            timestamps=[1, 2, 3],
            numeric_values={},
            string_values={"string": ["one", None, "three"]},
            binary_values={},  # to test Empty
            tag_values=None  # to test None
        )

        # assert
        expected_df = pandas.DataFrame([
            {"timestamp": 1, "string": "one"},
            {"timestamp": 2},
            {"timestamp": 3, "string": "three"}
        ])

        df_orig = timeseries_data.to_dataframe()
        assert_frame_equal(expected_df.sort_index(axis=1), df_orig.sort_index(axis=1), check_names=True)


    def test_convert_to_timeseriesdata(self):
        # arrange
        df = pandas.DataFrame([
            {"timestamp": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number": 0.123, "string": "string value", "binary": bytes("binary", "utf-8")}
        ])

        # act
        raw = TimeseriesDataRaw.from_dataframe(df)
        data = raw.convert_to_timeseriesdata()
        result = data.to_dataframe()

        # assert
        assert_frame_equal(df.sort_index(axis=1), result.sort_index(axis=1), check_names=True)

    def test_from_dataframe(self):
        # arrange
        df = pandas.DataFrame([
            {"timestamp": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number": 0.123, "string": "string value", "binary": bytes("binary", "utf-8")}
        ])

        # act
        raw = TimeseriesDataRaw.from_dataframe(df)
        raw_ptr = raw.get_net_pointer()
        reloaded_from_csharp = TimeseriesDataRaw(raw_ptr)  # this will force us to load data back from the assigned values in c# rather than checking against potentially cached vals

        # assert
        self.assertEqual(reloaded_from_csharp.timestamps, [1000000])
        self.assertEqual(reloaded_from_csharp.tag_values, {'tag1': ['tag1_value'], 'tag2': ['tag2_value']})
        self.assertEqual(reloaded_from_csharp.string_values, {'string': ['string value']})
        self.assertEqual(reloaded_from_csharp.numeric_values, {'number': [0.123]})
        self.assertEqual(reloaded_from_csharp.binary_values, {'binary': [b'binary']})
        self.assertEqual(reloaded_from_csharp.epoch, 0)

    def test_from_panda_dataframe_equals_to_panda_dataframe(self):
        # test whether the from_panda_dataframe -> to_panda_dataframe equals
        def _assert_test(pdf):

            def assertFrameEqual(df1, df2, **kwds ):
                """ Assert that two dataframes are equal, ignoring ordering of columns"""
                df1 = df1.reset_index(drop=True)
                df2 = df2.reset_index(drop=True)
                assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds )

            timeseries_data_raw = TimeseriesDataRaw.from_dataframe(pdf)
            assertFrameEqual(
                        pdf,
                        timeseries_data_raw.to_dataframe()
                    )


        _assert_test(pandas.DataFrame([{"timestamp": 1000000}]))
        _assert_test(pandas.DataFrame([{"timestamp": 1000000}, {"timestamp": 2000000}, {"timestamp": 3000000}]))


        _assert_test(pandas.DataFrame([
            {"timestamp": 1000000, "TAG__tag1": "tag1_value", "TAG__tag2": "tag2_value", "number1":0.123, "number2":0.123}
        ]))
        _assert_test(pandas.DataFrame([
            {"timestamp": 1000000, "TAG__tag1": "tag1_value_1", "TAG__tag2": "tag2_value_1", "number1":0.123, "number2":0.123},
            {"timestamp": 2000000, "TAG__tag1": "tag1_value_2", "TAG__tag2": "tag2_value_2", "number1":0.623, "number2":0.1312}
        ]))

        _assert_test(pandas.DataFrame([
            {"timestamp": 1000000, "TAG__tag1": "tag1_value_1", "TAG__tag2": "tag2_value_1", "number1": 0.123,
             "number2": 0.123},
            {"timestamp": 2000000, "TAG__tag1": "tag1_value_2", "TAG__tag2": "tag2_value_2", "number1": 0.623,
             "number2": 0.1312}
        ]).head(2).tail(1))

    def test_multiple_time_columns(self):
        def _assert_time(pdf, time):
            timeseries_data_raw = TimeseriesDataRaw.from_dataframe(pdf).to_dataframe()
            parsed_time=timeseries_data_raw.loc[0, 'timestamp']
            self.assertEqual(time, parsed_time)

        _assert_time(pandas.DataFrame([{"value": 0.1,"timestamp": 1000000}]), 1000000)
        _assert_time(pandas.DataFrame([{"TiMe": 2000000, "value": 0.1}]), 2000000)
        _assert_time(pandas.DataFrame([{"value": 0.1,"datetime": 3000000}]), 3000000)
        _assert_time(pandas.DataFrame([{"TiMeSTAMP": 5000000, "value": 0.1}]), 5000000)
        _assert_time(pandas.DataFrame([{"value": 0.1, "TiMeSTAMP": 5000000}]), 5000000)