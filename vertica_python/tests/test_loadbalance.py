from .base import VerticaPythonTestCase
from .. import errors

class TestLoadBalance(VerticaPythonTestCase):
    def tearDown(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = self._port

    def test_loadbalance_option_not_set(self):
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            pass

    def test_loadbalance_true(self):
        self._conn_info['connection_load_balance'] = True
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            pass

    def test_loadbalance_false(self):
        self._conn_info['connection_load_balance'] = False
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            pass

    def test_loadbalance_failover_first_host_port_invalid(self):
        self._conn_info['host'] = ['invalid', self._host]
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            pass

    def test_loadbalance_failover_both_host_port_invalid(self):
        self._conn_info['host'] = ['invalid', 'invalid']
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_loadbalance_when_no_host_port_are_provided(self):
        self._conn_info['host'] = []
        self._conn_info['port'] = []
        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_loadbalance_when_host_port_length_doesnt_match(self):
        self._conn_info['host'] = [self._host]
        self._conn_info['port'] = [self._port, self._port]
        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass