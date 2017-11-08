from .base import VerticaPythonIntegrationTestCase
from .. import errors
import unittest


class LoadBalanceTestCase(VerticaPythonIntegrationTestCase):
    def tearDown(self):
        self._conn_info['host'] = self._host
        self._conn_info['port'] = self._port
        self._conn_info['connection_load_balance'] = False
        self._conn_info['backup_server_node'] = []

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")

    def get_node_num(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM nodes WHERE node_state='UP'")
            db_node_num = cur.fetchone()[0]
            return db_node_num

    def test_loadbalance_option_disabled(self):
        if 'connection_load_balance' in self._conn_info:
            del self._conn_info['connection_load_balance']
        self.assertConnectionSuccess()

        self._conn_info['connection_load_balance'] = False
        self.assertConnectionSuccess()

    def test_loadbalance_random(self):
        self.require_DB_nodes_at_least(3)
        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('RANDOM')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")

            cur.execute("SELECT count(DISTINCT n)>1 FROM test_loadbalance")
            res = cur.fetchone()
            self.assertTrue(res[0])

    def test_loadbalance_none(self):
        # Client turns on connection_load_balance but server is unsupported
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('NONE')")
        self._conn_info['connection_load_balance'] = True

        # Client will proceed with the existing connection with initiator
        self.assertConnectionSuccess()

        # Test for multi-node DB
        self.require_DB_nodes_at_least(3)
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")

            cur.execute("SELECT count(DISTINCT n)=1 FROM test_loadbalance")
            res = cur.fetchone()
            self.assertTrue(res[0])

    def test_loadbalance_roundrobin(self):
        self.require_DB_nodes_at_least(3)
        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")

            cur.execute("SELECT count(n)=3 FROM test_loadbalance GROUP BY n")
            res = cur.fetchall()
            # verify that all db_node_num nodes are represented equally
            self.assertEqual(len(res), self.db_node_num)
            for i in res:
                self.assertEqual(i, [True])

    def test_failover_empty_backup(self):
        # Connect to primary server
        if 'backup_server_node' in self._conn_info:
            del self._conn_info['backup_server_node']
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = []
        self.assertConnectionSuccess()

        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # Fail to connect to primary server
        self.assertConnectionFail()

    def test_failover_one_backup(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # One valid address in backup_server_node
        self._conn_info['backup_server_node'] = [(self._host, self._port)]
        self.assertConnectionSuccess()

        # One invalid address in backup_server_node: DNS failed, Name or service not known
        self._conn_info['backup_server_node'] = [('invalidhost2', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: DNS failed, Name or service not known
        self._conn_info['backup_server_node'] = [('123.456.789.123', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: DNS failed, Address family for hostname not supported
        self._conn_info['backup_server_node'] = [('fd76:6572:7469:6361:0:242:ac11:4', 8888)]
        self.assertConnectionFail()

        # One invalid address in backup_server_node: Wrong port, connection refused
        self._conn_info['backup_server_node'] = [(self._host, 8888)]
        self.assertConnectionFail()

    def test_failover_multi_backup(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        # One valid and two invalid addresses in backup_server_node
        self._conn_info['backup_server_node'] = [(self._host, self._port), 'invalidhost2','foo']
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = ['foo', (self._host, self._port), ('123.456.789.1', 888)]
        self.assertConnectionSuccess()
        self._conn_info['backup_server_node'] = ['foo', ('invalidhost2', 8888), (self._host, self._port)]
        self.assertConnectionSuccess()

        # Three invalid addresses in backup_server_node
        self._conn_info['backup_server_node'] = ['foo', (self._host, 9999), ('123.456.789.1', 888)]
        self.assertConnectionFail()

    def test_failover_backup_format(self):
        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999

        err_msg = 'Connection option "backup_server_node" must be a list'
        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = (self._host, self._port)
            with self._connect() as conn:
                pass

        err_msg = ('Each item of connection option "backup_server_node"'
                   ' must be a host string or a \(host, port\) tuple')
        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = [9999]
            with self._connect() as conn:
                pass

        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = [(self._host, self._port, 'foo', 9999)]
            with self._connect() as conn:
                pass

        err_msg = 'Host .* must be a string and port .* must be an integer'
        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = [(self._host, 'port_num')]
            with self._connect() as conn:
                pass

        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = [(9999, self._port)]
            with self._connect() as conn:
                pass

        with self.assertRaisesRegexp(TypeError, err_msg):
            self._conn_info['backup_server_node'] = [(9999, 'port_num')]
            with self._connect() as conn:
                pass

    def test_failover_with_loadbalance_roundrobin(self):
        self.require_DB_nodes_at_least(3)

        # Set primary server to invalid host and port
        self._conn_info['host'] = 'invalidhost'
        self._conn_info['port'] = 9999
        self.assertConnectionFail()

        self._conn_info['backup_server_node'] = [('invalidhost2', 8888), (self._host, self._port)]
        self.assertConnectionSuccess()

        self._conn_info['connection_load_balance'] = True
        rowsToInsert = 3 * self.db_node_num

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT set_load_balance_policy('ROUNDROBIN')")
            cur.execute("DROP TABLE IF EXISTS test_loadbalance")
            cur.execute("CREATE TABLE test_loadbalance (n varchar)")
            # record which node the client has connected to
            for i in range(rowsToInsert):
                with self._connect() as conn1:
                    cur1 = conn1.cursor()
                    cur1.execute("INSERT INTO test_loadbalance (SELECT node_name FROM sessions "
                                 "WHERE session_id = (SELECT current_session()))")

            cur.execute("SELECT count(n)=3 FROM test_loadbalance GROUP BY n")
            res = cur.fetchall()
            # verify that all db_node_num nodes are represented equally
            self.assertEqual(len(res), self.db_node_num)
            for i in res:
                self.assertEqual(i, [True])
