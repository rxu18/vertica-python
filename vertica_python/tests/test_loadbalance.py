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
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")

    def tearDown(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = self._port
        if 'connection_load_balance' in self._conn_info:
            del self._conn_info['connection_load_balance']

    def test_loadbalance_option_not_set(self):
        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_loadbalance_roundrobin(self):
        self._conn_info['connection_load_balance'] = True
        rows = 9
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")
            cur.execute("CREATE TABLE test_loadbalanceADO (n varchar)")

            for i in range(rows):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalanceADO (SELECT node_name FROM sessions WHERE session_id = "
                                 "(SELECT current_session()))")

            cur.execute("SELECT count(n)=3 FROM test_loadbalanceADO GROUP BY n ")
            self.assertTrue(cur.fetchone())
            self.assertTrue(cur.fetchone())
            self.assertTrue(cur.fetchone())

            # teardown
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")

    def test_loadbalance_random(self):
        self._conn_info['connection_load_balance'] = True
        rows = 10
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('RANDOM')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")
            cur.execute("CREATE TABLE test_loadbalanceADO (n varchar)")

            for i in range(rows):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalanceADO (SELECT node_name FROM sessions WHERE session_id = "
                                 "(SELECT current_session()))")

            cur.execute("SELECT (count(DISTINCT nodes.node_name)=1) or (count(DISTINCT test_loadbalanceADO.n)=1)"
                        "  FROM nodes, test_loadbalanceADO")
            self.assertTrue(cur.fetchone())

            # teardown
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")

    def test_loadbalance_none(self):
        rows = 10
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")
            cur.execute("CREATE TABLE test_loadbalanceADO (n varchar)")

            for i in range(rows):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalanceADO (SELECT node_name FROM sessions WHERE session_id = "
                                 "(SELECT current_session()))")

            cur.execute("SELECT (count(DISTINCT test_loadbalanceADO.n)=1) FROM test_loadbalanceADO")
            self.assertTrue(cur.fetchone())

            # teardown
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalanceADO")

    def test_loadbalance_false(self):
        self._conn_info['connection_load_balance'] = False
        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_failover_first_host_port_incorrect(self):
        self._conn_info['host'] = ['incorrect', self._host]
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_failover_both_host_port_list_elements_incorrect(self):
        self._conn_info['host'] = ['incorrect', 'incorrect']
        port = self._conn_info['port']
        self._conn_info['port'] = [port, port]

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_port_invalid(self):
        self._conn_info['host'] = 0
        self._conn_info['port'] = 'invalid'

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_port_invalid(self):
        self._conn_info['port'] = 'invalid'

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_port_incorrect(self):
        self._conn_info['port'] = 9999

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_invalid(self):
        self._conn_info['host'] = 0

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_incorrect(self):
        self._conn_info['host'] = 'incorrect'

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_when_no_host_port_are_provided(self):
        self._conn_info['host'] = []
        self._conn_info['port'] = []
        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_when_host_port_length_doesnt_match(self):
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

        # Reset load balance back to roundrobin
        self._conn_info['connection_load_balance'] = False
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")

    def test_failover_host_str_port_list_type_mismatch(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = [self._port]

        with self.assertRaises(errors.ConnectionError):
            with self._connect():
                pass

    def test_failover_individual_host_port_str_type(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = str(self._port)

        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)

    def test_failover_list_host_port_str_type(self):
        self._conn_info['host'] = self._host + "," + self._host
        self._conn_info['port'] = str(self._port) + "," + str(self._port)

        with self._connect() as conn:
            self.assertIsNotNone(conn.socket)
