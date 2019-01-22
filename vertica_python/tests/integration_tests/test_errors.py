from __future__ import print_function, division, absolute_import

from .base import VerticaPythonIntegrationTestCase

from ... import errors


class ErrorTestCase(VerticaPythonIntegrationTestCase):
    def test_missing_schema(self):
        with self._connect() as conn:
            cur = conn.cursor()
            with self.assertRaises(errors.MissingSchema):
                cur.execute("SELECT 1 FROM missing_schema.table")

    def test_missing_relation(self):
        with self._connect() as conn:
            cur = conn.cursor()
            with self.assertRaises(errors.MissingRelation):
                cur.execute("SELECT 1 FROM missing_table")

    def test_duplicate_object(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS duplicate_table")
            query = "CREATE TABLE duplicate_table (a BOOLEAN)"
            cur.execute(query)
            with self.assertRaises(errors.DuplicateObject):
                cur.execute(query)
            cur.execute("DROP TABLE IF EXISTS duplicate_table")
