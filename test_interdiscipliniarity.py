import sys, os, time
import unittest

from interdisc import Interdisc
from DBConnectMAG import DBConnectMAG
from settings_db import DATABASE_SETTINGS

import pandas as pd
import numpy as np

venue_ids_fname = 'scripts/journal_list.csv'
venue_matrix_fname = 'scripts/mag_venue_sparse.mtx'

class TestCase(unittest.TestCase):
    def setUp(self):
        settings = DATABASE_SETTINGS['default']
        self.db = DBConnectMAG(**settings)
        self.iobj = Interdisc(self.db, venue_ids_fname, venue_matrix_fname)

    def tearDown(self):
        self.db = None
        self.iobj = None

    def test_sizes(self):
        numrows = 24108
        self.assertEqual(self.iobj.venue_matrix.shape, (numrows, numrows),
                            'unexpected matrix shape')
        self.assertEqual(len(self.iobj.venue_ids), numrows,
                            'unexpected number of venue ids {}'.format(len(self.iobj.venue_ids)))

    def test_query_citations(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            for direction in ['out', 'in']:
                df = self.db.query_for_citations(pid, direction=direction, return_df=True)
                self.assertFalse(df.empty)
                self.assertEqual(df.shape[1], 2, 'expected 2 columns')
        for pid in test_pids:
            s = self.db.query_for_citations(pid, direction='out')
            self.assertTrue((type(s)==pd.Series))
            self.assertEqual(len(s), 14)
        for pid in test_pids:
            s = self.db.query_for_citations(pid, direction='in')
            self.assertTrue((type(s)==pd.Series))
            self.assertEqual(len(s), 3)

    def test_count_venues(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            counts = self.iobj.get_venue_counts(pid)
            self.assertTrue(type(counts)==pd.Series)
            self.assertEqual(len(counts), len(self.iobj.venue_ids))

    def test_get_scores_from_papers(self):
        test_pids = [1889115760, '1889115760', [1889115760], pd.Series(1889115760)]
        for pid in test_pids:
            scores = self.iobj.get_scores_from_paperids(pid)
            self.assertTrue(len(scores)==2)
            for score in scores:
                self.assertTrue(score>0 and score<1)
        


if __name__ == "__main__":
    unittest.main()
