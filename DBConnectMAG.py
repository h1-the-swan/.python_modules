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
            table=self.tblname_nodes, 
            col_venue=self.colname_venue[0], 
            col_paperid=self.colname_paperid):
        """TODO: given a paperid or list of paperids, return the counts by venue
        Venues are e.g. journals, and is specified by the 'col_venue' param

        :paperids: paperid or list of paperids
        :table: table object or name of table
        :col_venue: column (or name of column) to count
        :col_paper: column (or name of column) with paper ids
        :returns: SQLAchemy response

        """
        paperids = parse_id(paperids)
        tbl = self._get_table(table)
        col_venue = self._get_col(col_venue)
        col_paperid = self._get_col(col_paperid)
        sq = tbl.count(col_paperid.in_(paperids))
        sq = sq.where(col_venue>0)  # TODO: THIS MAY NOT WORK WITH STRING IDS
        sq = sq.group_by(col_venue)
        sq = sq.column(col_venue)
        r = self.db.engine.execute(sq)
        return r
