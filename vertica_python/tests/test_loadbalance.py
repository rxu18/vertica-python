from .base import VerticaPythonTestCase
from .. import errors

class LoadBalanceTestCase(VerticaPythonTestCase):
    @classmethod
    def setUpClass(cls):
        super(LoadBalanceTestCase, cls).setUpClass()
        with cls._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")

    @classmethod
    def tearDownClass(cls):
        super(LoadBalanceTestCase, cls).tearDownClass()
        with cls._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")

    def tearDown(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = self._port
        if 'connection_load_balance' in self._conn_info:
            del self._conn_info['connection_load_balance']

    def test_loadbalance_option_not_set(self):
        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_loadbalance_true(self):
        self._conn_info['connection_load_balance'] = True
        with self._connect() as conn1, self._connect() as conn2:
            cur1 = conn1.cursor()
            cur1.execute('SELECT node_name from current_session')
            node1 = cur1.fetchone()

            cur2 = conn2.cursor()
            cur2.execute('SELECT node_name from current_session')
            node2 = cur2.fetchone()

            self.assertNotEqual(node1, node2)

    def test_loadbalance_false(self):
        self._conn_info['connection_load_balance'] = False
        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_failover_first_host_port_invalid(self):
        self._conn_info['host'] = ['invalid', self._host]
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_failover_both_host_port_list_elements_invalid(self):
        self._conn_info['host'] = ['invalid', 'invalid']
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_port_invalid(self):
        self._conn_info['host'] = 'invalid'
        self._conn_info['port'] = 'invalid'

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_port_invalid(self):
        self._conn_info['port'] = 'invalid'

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_invalid(self):
        self._conn_info['host'] = 'invalid'

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

    def test_loadbalance_when_server_doesnt_support_loadbalance(self):
        self._conn_info['connection_load_balance'] = True

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('None')")

        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

        #Reset load balance back to roundrobin
        self._conn_info['connection_load_balance'] = False
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")

    def test_loadbalance_host_str_port_list_type_mismatch(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = [self._port]

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_loadbalance_host_str_port_str_type_mismatch(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = str(self._port)

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass
