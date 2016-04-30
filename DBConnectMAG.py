from MySQLConnect import MySQLConnect
from citation_db_funcs import parse_id

class DBConnectMAG(MySQLConnect):
    """Extends the MySQLConnect object with MAG (Microsoft Academic Graph) specific things

    :**kwargs: keyword arguments to be passed to MySQLConnect
    kwargs['db_name'] defaults to 'MAG_20160205' if not specified
    """
    def __init__(self, **kwargs):
        if not kwargs.get('db_name'):
            kwargs['db_name'] = 'MAG_20160205'
        MySQLConnect.__init__(self, **kwargs)

        self.tblname_nodes = 'Papers'
        self.colname_venue = ['Journal_ID_mapped_to_venue_name', 'Conference_series_ID_mapped_to_venue_name']
        self.colname_paperid = 'Paper_ID'
        
        self.tblname_links = 'PaperReferences'
        self.colname_citing = 'Paper_ID'
        self.colname_cited = 'Paper_reference_ID'

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

    def query_for_venue_counts(self, paperids, 
            table=None, 
            col_venue=None, 
            col_paperid=None):
        """given a paperid or list of paperids, return the counts by venue
        Venues are e.g. journals, specified by the 'col_venue' param

        :paperids: paperid or list of paperids
        :table: table object or name of table
        :col_venue: column (or name of column) to count
        :col_paper: column (or name of column) with paper ids
        :returns: SQLAchemy response with columns (count, venue_id)

        """
        if table is None:
            table=self.tblname_nodes
        if col_venue is None:
            col_venue = self.colname_venue[0]
        if col_paperid is None:
            col_paperid = self.colname_paperid
        paperids = parse_id(paperids)
        tbl = self._get_table(table)
        col_venue = self._get_col(col_venue, tbl)
        col_paperid = self._get_col(col_paperid, tbl)
        sq = tbl.count(col_paperid.in_(paperids))
        sq = sq.where(col_venue>0)  # TODO: THIS MAY NOT WORK WITH STRING IDS
        sq = sq.group_by(col_venue)
        sq = sq.column(col_venue)
        r = self.engine.execute(sq)
        return r

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
