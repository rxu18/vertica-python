from __future__ import print_function, division, absolute_import

import getpass
from .base import VerticaPythonIntegrationTestCase


class ConnectionTestCase(VerticaPythonIntegrationTestCase):
    def test_client_os_user_name_metadata(self):
        value = getpass.getuser()

        # Metadata client_os_user_name sent from client should be captured into system tables
        query = 'SELECT client_os_user_name FROM v_monitor.current_session'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_monitor.sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_monitor.user_sessions WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

        query = 'SELECT client_os_user_name FROM v_internal.dc_session_starts WHERE session_id=(SELECT current_session())'
        res = self._query_and_fetchone(query)
        self.assertEqual(res[0], value)

