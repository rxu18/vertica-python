# Copyright (c) 2018-2019 Micro Focus or one of its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, division, absolute_import

from .base import VerticaPythonIntegrationTestCase
import six
import subprocess


class AuthenticationTestCase(VerticaPythonIntegrationTestCase):
    def setUp(self):
        super(AuthenticationTestCase, self).setUp()
        self._user = self._conn_info['user']
        self._password = self._conn_info['password']

    def tearDown(self):
        self._conn_info['user'] = self._user
        self._conn_info['password'] = self._password
        super(AuthenticationTestCase, self).tearDown()

    def test_kerberos(self):
        if six.PY2:
            sp = subprocess.call
        else:
            sp = subprocess.run
        if not self.test_config['enable_kerberos_test']:
            msg = ("The test uses the Dockerized testing environment.")
            self.skipTest(msg)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS user1")
            cur.execute("DROP AUTHENTICATION IF EXISTS testkerberos CASCADE")
            try:
                cur.execute("CREATE USER user1")
                cur.execute("CREATE AUTHENTICATION testkerberos METHOD 'gss' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION testkerberos TO user1")
                sp(["user1", "|", "kinit", "user1"])
                self._conn_info['user'] = 'user1'
                self.assertConnectionSuccess()
                # TODO: Add all other tests to the framework. This includes Test Expiration, Test No TGT and Test Wrong Realm.
            finally:
                cur.execute("DROP USER IF EXISTS krb_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testkerberos CASCADE")

    def test_SHA512(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS sha512_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testhostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")
            try:
                cur.execute("CREATE USER sha512_user IDENTIFIED BY 'password'")
                cur.execute("ALTER USER sha512_user SECURITY_ALGORITHM 'SHA512'")
                cur.execute("CREATE AUTHENTICATION testhostHash METHOD 'hash' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION testhostHash TO sha512_user")
                cur.execute("CREATE AUTHENTICATION testlocalHash METHOD 'hash' LOCAL")
                cur.execute("GRANT AUTHENTICATION testlocalHash TO sha512_user")

                self._conn_info['user'] = 'sha512_user'
                self._conn_info['password'] = 'password'

                expire_msg = 'The password for user sha512_user has expired'
                self.assertConnectionFail(err_msg=expire_msg)

                # Test SHA512 connection
                cur.execute("ALTER USER sha512_user IDENTIFIED BY 'password'")
                self.assertConnectionSuccess()

                # Switch to MD5 for hashing and storing the user password
                cur.execute("ALTER USER sha512_user SECURITY_ALGORITHM 'MD5'")
                self.assertConnectionFail(err_msg=expire_msg)

                # Test MD5 connection
                cur.execute("ALTER USER sha512_user IDENTIFIED BY 'password'")
                self.assertConnectionSuccess()
            finally:
                # Must clean up authentication methods within this session, no
                # matter whether an exception has occurred or not, otherwise
                # those authentication methods may affect existing users
                cur.execute("DROP USER IF EXISTS sha512_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testhostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")

    def test_trust(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS trust_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testtrust CASCADE")
            try:
                cur.execute("CREATE USER trust_user")
                cur.execute("CREATE AUTHENTICATION testtrust METHOD 'trust' HOST '0.0.0.0/0'")
                cur.execute ("GRANT AUTHENTICATION testtrust TO trust_user")
                self._conn_info['user'] = 'trust_user'
                self.assertConnectionSuccess()
            finally:
                cur.execute("DROP USER IF EXISTS trust_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testtrust CASCADE")

    def test_reject(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS reject_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testreject CASCADE")
            try:
                cur.execute("CREATE USER reject_user")
                cur.execute("CREATE AUTHENTICATION testreject METHOD 'reject' HOST '0.0.0.0/0'")
                cur.execute ("GRANT AUTHENTICATION testreject TO reject_user")
                self._conn_info['user'] = 'reject_user'
                self.assertConnectionFail(err_msg='.*')
            finally:
                cur.execute("DROP USER IF EXISTS reject_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testreject CASCADE")

    def test_password_expire(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP USER IF EXISTS pw_expire_user")
            cur.execute("DROP AUTHENTICATION IF EXISTS testhostHash CASCADE")
            cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")
            try:
                cur.execute("CREATE USER pw_expire_user IDENTIFIED BY 'password'")
                cur.execute("CREATE AUTHENTICATION testhostHash METHOD 'hash' HOST '0.0.0.0/0'")
                cur.execute("GRANT AUTHENTICATION testhostHash TO pw_expire_user")
                cur.execute("CREATE AUTHENTICATION testlocalHash METHOD 'hash' LOCAL")
                cur.execute("GRANT AUTHENTICATION testlocalHash TO pw_expire_user")

                # Test connection
                self._conn_info['user'] = 'pw_expire_user'
                self._conn_info['password'] = 'password'
                self.assertConnectionSuccess()

                # Expire the user's password immediately
                cur.execute("ALTER USER pw_expire_user PASSWORD EXPIRE")

                # Expect an error message
                self.assertConnectionFail(
                    err_msg='The password for user pw_expire_user has expired')
            finally:
                # Must clean up authentication methods within this session, no
                # matter whether an exception has occurred or not, otherwise
                # those authentication methods may affect existing users
                cur.execute("DROP USER IF EXISTS pw_expire_user")
                cur.execute("DROP AUTHENTICATION IF EXISTS testhostHash CASCADE")
                cur.execute("DROP AUTHENTICATION IF EXISTS testlocalHash CASCADE")


exec(AuthenticationTestCase.createPrepStmtClass())
