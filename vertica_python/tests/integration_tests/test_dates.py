from __future__ import print_function, division, absolute_import

from collections import namedtuple
from datetime import date

from .base import VerticaPythonIntegrationTestCase
from ... import errors

DateTestingCase = namedtuple("DateTestingCase", ["string", "template", "date"])


class DateParsingTestCase(VerticaPythonIntegrationTestCase):
    """Testing DATE type parsing with focus on 'AD'/'BC'.

    Note: the 'BC' or 'AD' era indicators in Vertica's date format seem to make Vertica behave as
    follows:
        1. Both 'BC' and 'AD' are simply a flags that tell Vertica: include era indicator if the
        date is Before Christ
        2. Dates in 'AD' will never include era indicator
    """

    def _test_dates(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                res = cur.fetchall()
                self.assertListOfListsEqual(res, [[tc.date]], msg=msg)

    def _test_not_supported(self, test_cases, msg=None):
        with self._connect() as conn:
            cur = conn.cursor()
            for tc in test_cases:
                with self.assertRaises(errors.NotSupportedError, msg=msg):
                    cur.execute("SELECT TO_DATE('{0}', '{1}')".format(tc.string, tc.template))
                    res = cur.fetchall()
                    self.assertListOfListsEqual(res, [[tc.date]])

    def test_no_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> no indicator')

    def test_ad_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> no indicator')

    def test_bc_to_no(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='BC indicator -> no indicator')

    def test_no_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> AD indicator')

    def test_ad_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> AD indicator')

    def test_bc_to_ad(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD AD', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD AD', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD AD', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD AD', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> AD indicator')

    def test_no_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='no indicator -> BC indicator')

    def test_ad_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 AD', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 AD', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 AD', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 AD', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_dates(test_cases=test_cases, msg='AD indicator -> BC indicator')

    def test_bc_to_bc(self):
        test_cases = [
            DateTestingCase('1985-10-25 BC', 'YYYY-MM-DD BC', date(1985, 10, 25)),
            DateTestingCase('1955-11-12 BC', 'YYYY-MM-DD BC', date(1955, 11, 12)),
            DateTestingCase('1885-01-01 BC', 'YYYY-MM-DD BC', date(1885, 1, 1)),
            DateTestingCase('2015-10-21 BC', 'YYYY-MM-DD BC', date(2015, 10, 21)),
        ]
        self._test_not_supported(test_cases=test_cases, msg='BC indicator -> BC indicator')

