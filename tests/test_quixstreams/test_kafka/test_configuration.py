import os
from unittest.mock import patch

import pydantic
import pytest

from quixstreams.kafka.configuration import ConnectionConfig


class TestConnectionConfig:
    def test_literal_casings(self):
        """
        Literals are case-sensitive, so confirm casing is automatically adjusted
        for all of them
        """
        config = ConnectionConfig(
            bootstrap_servers="url",
            sasl_mechanism="gssAPI",
            security_protocol="PLAINtext",
            sasl_oauthbearer_method="deFAULt",
            ssl_endpoint_identification_algorithm="HttpS",
        )
        assert config.sasl_mechanism == "GSSAPI"
        assert config.security_protocol == "plaintext"
        assert config.sasl_oauthbearer_method == "default"
        assert config.ssl_endpoint_identification_algorithm == "https"

    @pytest.mark.parametrize("mechanism_casing", ["plain", "PLAIN"])
    def test_from_librdkafka_dict(self, mechanism_casing):
        librdkafka_dict = {
            "bootstrap.servers": "url",
            "sasl.mechanism": mechanism_casing,
            "sasl.username": "my-username",
        }
        config = ConnectionConfig.from_librdkafka_dict(librdkafka_dict)

        assert config.bootstrap_servers == librdkafka_dict["bootstrap.servers"]
        assert config.sasl_mechanism == librdkafka_dict["sasl.mechanism"].upper()
        assert config.sasl_username == librdkafka_dict["sasl.username"]

    def test_from_librdkafka_dict_extras_raise(self):
        librdkafka_dict = {
            "bootstrap.servers": "url",
            "sasl.username": "my-username",
            "not.a.setting": "oh_no",
        }

        with pytest.raises(pydantic.ValidationError):
            ConnectionConfig.from_librdkafka_dict(librdkafka_dict)

    def test_from_librdkafka_dict_ignore_extras(self):
        librdkafka_dict = {
            "bootstrap.servers": "url",
            "sasl.username": "my-username",
            "not.a.setting": "oh_no",
        }

        config = ConnectionConfig.from_librdkafka_dict(
            librdkafka_dict, ignore_extras=True
        )
        assert config.bootstrap_servers == "url"
        assert config.sasl_username == "my-username"

    def test_from_librdkafka_dict_ignore_extras_keeps_alias(self):
        librdkafka_dict = {
            "bootstrap.servers": "url",
            "sasl.mechanisms": "PLAIN",
            "not.a.setting": "oh_no",
        }

        config = ConnectionConfig.from_librdkafka_dict(
            librdkafka_dict, ignore_extras=True
        )
        assert config.bootstrap_servers == "url"
        assert config.sasl_mechanism == "PLAIN"

    def test_sasl_mechanism_aliases(self):
        """
        "sasl_mechanisms" should be converted to "sasl_mechanism"
        """
        mechanism = ConnectionConfig(bootstrap_servers="url", sasl_mechanism="PLAIN")
        mechanisms = ConnectionConfig(bootstrap_servers="url", sasl_mechanisms="plain")
        mechanism_d = ConnectionConfig.from_librdkafka_dict(
            {"bootstrap.servers": "url", "sasl.mechanism": "Plain"}
        )
        mechanisms_d = ConnectionConfig.from_librdkafka_dict(
            {"bootstrap.servers": "url", "sasl.mechanisms": "PlaiN"}
        )

        assert mechanism == mechanisms == mechanism_d == mechanisms_d
        assert mechanism.sasl_mechanism == "PLAIN"
        with pytest.raises(AttributeError):
            mechanisms.sasl_mechanisms

        d = mechanism.as_librdkafka_dict()
        assert "sasl.mechanism" in d
        assert "sasl.mechanisms" not in d

    def test_secret_field(self):
        """
        Confirm a secret field is obscured
        """
        config = ConnectionConfig(bootstrap_servers="url", sasl_password="blah")
        assert config.sasl_password != "blah"
        assert config.sasl_password.get_secret_value() == "blah"

    def test_str(self):
        """
        String representation has obscured secrets
        """
        config = ConnectionConfig(bootstrap_servers="url", sasl_password="blah")
        print_cfg = str(config)
        assert "blah" not in print_cfg
        assert "****" in print_cfg

    def test_as_librdkafka_dict(self):
        config = ConnectionConfig(bootstrap_servers="url", sasl_mechanism="PLAIN")
        librdkafka_dict = config.as_librdkafka_dict()
        assert librdkafka_dict["bootstrap.servers"] == config.bootstrap_servers
        assert librdkafka_dict["sasl.mechanism"] == config.sasl_mechanism

    def test_as_librdkafka_dict_plain_secret(self):
        password = "my-password"
        config = ConnectionConfig(
            bootstrap_servers="url", sasl_password=password, ssl_key_password=password
        )
        librdkafka_dict = config.as_librdkafka_dict(plaintext_secrets=True)
        assert librdkafka_dict["sasl.password"] == password
        assert librdkafka_dict["ssl.key.password"] == password

    def test_environment_not_read(self):
        with patch.dict(os.environ, {"SASL_PASSWORD": "cool_pw"}):
            config = ConnectionConfig(bootstrap_servers="url")
        assert config.sasl_password is None
