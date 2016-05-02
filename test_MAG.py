import sys, os, time
import unittest

from DBConnectMAG import DBConnectMAG
from settings_db import DATABASE_SETTINGS

import pandas as pd
import numpy as np

class TestCase(unittest.TestCase):
    def setUp(self):
        settings = DATABASE_SETTINGS['default']
        self.db = DBConnectMAG(**settings)

    def tearDown(self):
        self.db = None

    def test_query_field_of_study_for_paperid(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            fosids = self.db.query_field_of_study_for_paperid(pid)
            self.assertTrue(type(fosids)==list)
            self.assertTrue(isinstance(fosids[0], basestring))
            self.assertEqual(len(fosids), 3)

    def test_query_toplevel_fields_for_field_ids(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            fos_counts = self.db.query_toplevel_fields_for_field_ids(pid)
            self.assertTrue(isinstance(fos_counts.keys()[0], basestring))

    def test_query_field_of_study_name(self):
        fos_ids = [1306188, '1306188', [1306188]]
        for fosid in fos_ids:
            result = self.db.query_field_of_study_name(fosid)
            self.assertTrue(isinstance(result, basestring))
        fos_ids = [1306188, 99580578, 200823845]
        result = self.db.query_field_of_study_name(fos_ids)
        self.assertTrue(isinstance(result, dict))

    def test_get_single_field_for_paper(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            result = self.db.get_single_field_for_paper(pid)
            self.assertTrue(isinstance(result, dict))
            self.assertEqual(len(result), 2)

    def test_get_paperids_from_authorid(self):
        test_aids = [211222391, '211222391', [211222391], pd.Series(211222391)]
        for aid in test_aids:
            result = self.db.get_paperids_from_authorid(aid)
            self.assertEqual(type(result), pd.Series)
            self.assertEqual(len(result), 26)
            result = self.db.get_paperids_from_authorid(aid, return_df=True)
            self.assertEqual(type(result), pd.DataFrame)
            self.assertEqual(len(result), 26)

if __name__ == "__main__":
    unittest.main()

