import os

class TestHelper():

    @staticmethod
    def get_certpath() -> str:
        return os.path.abspath(os.path.join(__file__, "../../../../../TestCertificates/ca.cert"))

