from base import VerticaPythonTestCase

class TestLoadBalance(VerticaPythonTestCase):
    def test_loadbalance_option_not_set(self):
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            self.assertTrue(not conn.socket._closed)
            pass

    def test_loadbalance_true(self):
        self._conn_info['load'] = True
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            self.assertTrue(not conn.socket._closed)
            pass

    def test_loadbalance_false(self):
        self._conn_info['load'] = False
        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            self.assertTrue(not conn.socket._closed)
            pass

    def test_loadbalance_failover_first_host_port_invalid(self):
        self._conn_info['host'] = ['invalid', 'engdev3']
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self._connect() as conn:
            self.assertTrue(conn.socket is not None)
            self.assertTrue(not conn.socket._closed)
            pass
