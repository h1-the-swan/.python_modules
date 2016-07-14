from MySQLConnect import MySQLConnect
from citation_db_funcs import parse_id
from collections import Counter
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import string
import pandas as pd

class DBConnectJSTOR(MySQLConnect):
    """Extends the MySQLConnect object with JSTOR specific things

    :**kwargs: keyword arguments to be passed to MySQLConnect
    kwargs['db_name'] defaults to 'jp_jstor' if not specified
    """
    def __init__(self, **kwargs):
        if not kwargs.get('db_name'):
            kwargs['db_name'] = 'jp_jstor'
        MySQLConnect.__init__(self, **kwargs)

        self.tblname_nodes = 'nodes'
        self.colname_venue = ['jID']
        self.colname_paperid = 'pID'
        self.colname_year = 'year'
        self.colname_title = 'title'
        
        self.tblname_links = 'links'
        self.colname_citing = 'citing'
        self.colname_cited = 'cited'


    def _get_table(self, t):
        """If passed a string, get the table object

        :t: If a string, return the table object. Otherwise this method will do nothing
        :returns: table object

        """
        if isinstance(t, basestring):
            return self.tables[t]
        else:
            return t

    def _get_col(self, col, tbl):
        """If passed a string, get the column object

        :col: If a string, return the column object. Otherwise this method will do nothing
        :tbl: the table object that the column belongs to
        :returns: column object

        """
        if isinstance(col, basestring):
            return tbl.c[col]
        else:
            return col

    def query_for_citations(self, paperids, 
            direction='out', 
            table=None, 
            col_citing=None, 
            col_cited=None, 
            return_df=False):
        """Take a list of paper ids and return the citations as either a Series or DataFrame

        :paperids: paperid or list of paperids
        :direction: 'out' (default) for out-citations. 'in' for incoming citations
        :table: table object or name of table
        :col_citing: column (or name of column) for citing papers
        :col_cited: column (or name of column) for cited papers
        :return_df: return a dataframe rather than a series (default False)
        :returns: either a Series of paper IDs or the DataFrame of both citing/cited

        """
        if table is None:
            table = self.tblname_links
        if col_citing is None:
            col_citing = self.colname_citing
        if col_cited is None:
            col_cited = self.colname_cited
        paperids = parse_id(paperids)
        tbl = self._get_table(table)
        col_citing = self._get_col(col_citing, tbl)
        col_cited = self._get_col(col_cited, tbl)
        cols = (col_citing, col_cited)
        if direction.lower() == 'in':
            cols = (col_cited, col_citing)
        sq = tbl.select(cols[0].in_(paperids))
        sq = sq.with_only_columns(cols)
        result = self.read_sql(sq)
        if return_df is True:
            return result
        else:
            return result.ix[:, 1]
