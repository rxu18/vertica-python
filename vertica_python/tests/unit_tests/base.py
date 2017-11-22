from __future__ import print_function, division, absolute_import

from nose.plugins.attrib import attr
from ..common.base import VerticaPythonTestCase

@attr('unit_tests')
class VerticaPythonUnitTestCase(VerticaPythonTestCase):
    """
    Base class for tests that do not require database connection;
    simple unit testing of individual classes and functions
    """
    @classmethod
    def setUpClass(cls):
        cls.test_config = cls._load_test_config(['log_dir', 'log_level'])
        cls._setup_logger('unit_tests', cls.test_config['log_dir'], cls.test_config['log_level'])

    @classmethod
    def tearDownClass(cls):
        pass


