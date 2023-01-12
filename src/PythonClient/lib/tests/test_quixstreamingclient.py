import unittest
import os
from datetime import timedelta

from quixstreaming import QuixStreamingClient


class StreamingClientTests(unittest.TestCase):

    def test_constructor_with_notoken(self):
        # Act
        try:
            sc = QuixStreamingClient()
        except Exception as e:
            self.assertEqual(e.Message, 'Token must be given as an argument or set in Quix__Sdk__Token environment variable.')
            return

        raise Exception("This shouldn't be hit")

    def test_constructor_with_tokenfromenv(self):
        # Act
        os.environ["Quix__Sdk__token"] = "mytoken"
        sc = QuixStreamingClient()
        # Assert by no exception

    def test_constructor_with_token(self):
        # Act
        sc = QuixStreamingClient("mytoken");
        # Assert by no exception

    def test_constructor_with_autocreatetopics(self):
        # Act
        sc = QuixStreamingClient("mytoken", auto_create_topics=False)
        # Assert by no exception

    def test_constructor_with_properties(self):
        # Act
        sc = QuixStreamingClient("mytoken", properties={"acks": "0"})
        # Assert by no exception

    def test_constructor_with_debug(self):
        # Act
        sc = QuixStreamingClient("mytoken", debug=True)
        # Assert by no exception

    def test_TokenValidationConfiguration_shouldBeSetCorrectly(self):
        # Act
        sc = QuixStreamingClient("mytoken")

        # Assert
        sc.token_validation_config.enabled = False
        self.assertEqual(False, sc.token_validation_config.enabled)
        sc.token_validation_config.enabled = True
        self.assertEqual(True, sc.token_validation_config.enabled)

        sc.token_validation_config.warn_about_pat_token = False
        self.assertEqual(False, sc.token_validation_config.warn_about_pat_token)
        sc.token_validation_config.warn_about_pat_token = True
        self.assertEqual(True, sc.token_validation_config.warn_about_pat_token)

        sc.token_validation_config.warning_before_expiry = timedelta(1)
        self.assertEqual(timedelta(1), sc.token_validation_config.warning_before_expiry)
        sc.token_validation_config.warning_before_expiry = timedelta(2)
        self.assertEqual(timedelta(2), sc.token_validation_config.warning_before_expiry)

    def test_apiurl_getset(self):
        sc = QuixStreamingClient("mytoken")

        # Act
        sc.api_url = "https://test.quix.ai"

        # Assert
        self.assertEqual("https://test.quix.ai/", sc.api_url)

    def test_open_topic(self):
        os.environ["Quix__Sdk__Token"] = "something"
        sc = QuixStreamingClient()
        sc.api_url = "https://test.quix.ai"
        # Act
        try:
            sc.open_input_topic('sometest')
        # Assert
        except Exception as ex:
            # point here is it can fail inside c#, but not python
            self.assertEqual(ex.Message, "An error occurred while sending the request.")

    def test_cache_period_getset(self):
        # Act
        sc = QuixStreamingClient("mytoken")

        # Assert
        sc.cache_period = timedelta(1)
        self.assertEqual(timedelta(1), sc.cache_period)
