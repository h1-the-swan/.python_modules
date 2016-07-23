from MySQLConnect import MySQLConnect
from citation_db_funcs import parse_id
from collections import Counter
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import string
import pandas as pd

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
        self.colname_year = 'Paper_publish_year'
        self.colname_title = 'Original_paper_title'
        
        self.tblname_links = 'PaperReferences'
        self.colname_citing = 'Paper_ID'
        self.colname_cited = 'Paper_reference_ID'

        self.tblname_paper_fos = 'PaperKeywords'
        self.colname_paperfield = 'Field_of_study_ID_mapped_to_keyword'
        self.colname_fosweight = 'Confidence'

        self.tblname_toplevelfield = 'FieldOfStudyTopLevel'
        self.colname_fosid = 'Field_of_study_ID'
        self.colname_toplevel = 'Toplevel_field_ID'

        self.tblname_fields = 'FieldsOfStudy'
        self.colname_fosname = 'Field_of_study_name'

        self.tblname_paper_authors = 'PaperAuthorAffiliations'
        self.colname_authorid = 'Author_ID'

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

    def query_field_of_study_for_paperid(self, paperid,
                        table=None,
                        col_paperid=None,
                        col_fos=None):
        """Given a paper ID, return a list of Field of Study (FOS) IDs

        :paperid: paper ID (will also work with a list of paper IDs)
        :table: table object or name of table mapping papers to FOS
        :col_paperid: column (or name of column) for paper ID in the FOS table
        :col_fos: column (or name of column) for Field of Study ID in the FOS table
        :returns: list of FOS IDs (strings)

        """
        if table is None:
            table = self.tblname_paper_fos
        if col_paperid is None:
            col_paperid = self.colname_paperid
        if col_fos is None:
            col_fos = self.colname_paperfield
        tbl = self._get_table(table)
        col_paperid = self._get_col(col_paperid, tbl)
        col_fos = self._get_col(col_fos, tbl)

        paperid = parse_id(paperid)

        # get a list of FOS IDs
        sq = tbl.select().with_only_columns([col_fos])
        sq = sq.where(col_paperid.in_(paperid))
        r = self.engine.execute(sq).fetchall()
        return [str(row[0]) for row in r]

    def query_toplevel_fields_for_field_ids(self, fosids, 
                        table=None,
                        col_fosid=None,
                        col_toplevelid=None,
                        weighted=True,
                        col_weight=None):
        """Given a list of Field of Study (FOS) IDs, return a counter of toplevel FOS IDs with weights

        :fosids: Field of Study ID or list
        :table: table object or name of table mapping FOS IDs to Top Level IDs
        :col_fosid: column (or name of column) in the toplevel table for (lower level) FOS
        :col_toplevelid: column (or name of column) in the toplevel table for top level FOS
        :weighted: if true, will use the value of `col_weight` column in counting FOS. Default True
        :col_weight: column (or name of column) in the toplevel table with weights (e.g. 'Confidence')
        :returns: counter with keys: (toplevel) Field of Study ID and values: weights

        """
        if table is None:
            table = self.tblname_toplevelfield
        if col_fosid is None:
            col_fosid = self.colname_fosid
        if col_toplevelid is None:
            col_toplevelid = self.colname_toplevel
        if weighted:
            if col_weight is None:
                col_weight = self.colname_fosweight
        tbl = self._get_table(table)
        col_fosid = self._get_col(col_fosid, tbl)
        col_toplevelid = self._get_col(col_toplevelid, tbl)
        if weighted:
            col_weight = self._get_col(col_weight, tbl)

        fosids = parse_id(fosids)

        sq = tbl.select().with_only_columns([col_fosid, col_toplevelid])
        if weighted:
            sq = sq.column(col_weight)
        sq = sq.where(col_fosid.in_(fosids))
        r = self.engine.execute(sq)

        fos_count = Counter()
        matched = []
        if r.rowcount > 0:
            for i in range(r.rowcount):
                row = r.fetchone()
                matched.append(str(row[col_fosid]))
                if weighted:
                    fos_count[str(row[col_toplevelid])] += float(row[col_weight])
                else:
                    fos_count[str(row[col_toplevelid])] += 1
        # assume any leftovers are top level and should be added back in
        for fosid in fosids:
            if fosid not in matched:
                fos_count[fosid] += 1
        return fos_count

    def query_field_of_study_name(self, fosid,
                        squeeze=True,
                        table=None,
                        col_fosid=None,
                        col_fosname=None):
        """Given a Field of Study (FOS) ID, return the name of that field
        If a single fosid is given, return a string. If multiple fosids are given, return a dictionary

        :fosid: Field of Study ID or list
        :squeeze: if there is only one FOS ID, return just the name instead of a dictionary
        :table: table object or name of table with FOS IDs and names
        :col_fosid: column (or name of column) in the table for FOS
        :col_fosname: column (or name of column) in the table for FOS name
        :returns: FOS name or names as string or dictionary with keys FOS ID and values FOS name

        """
        if table is None:
            table = self.tblname_fields
        if col_fosid is None:
            col_fosid = self.colname_fosid
        if col_fosname is None:
            col_fosname = self.colname_fosname

        tbl = self._get_table(table)
        col_fosid = self._get_col(col_fosid, tbl)
        col_fosname = self._get_col(col_fosname, tbl)

        fosid = parse_id(fosid)

        sq = tbl.select(col_fosid.in_(fosid))
        sq = sq.with_only_columns([col_fosid, col_fosname])
        r = self.engine.execute(sq).fetchall()
        if len(fosid)==1 and squeeze is True:
            if len(r)==1:
                return r[0][col_fosname]
        else:
            return {row[col_fosid]: row[col_fosname] for row in r}

    def query_nodes(self, paperids,
                        table=None,
                        col_paperid=None,
                        return_type='dataframe'):
        """Given a paper ID or list, return results of querying the nodes table (title, publication year, etc.)

        :paperids: Paper ID or list
        :table: table object or name of table for nodes (papers)
        :col_paperid: column (or name of column) in the table for Paper ID
        :return_type: {'dataframe', 'dict'} if 'dataframe' (default) return a
        dataframe. if 'dict' return dictionary: {index -> {column -> value}}
        :returns: nodes dataframe or dictionary

        """
        if table is None:
            table=self.tblname_nodes
        if col_paperid is None:
            col_paperid = self.colname_paperid

        tbl = self._get_table(table)
        col_paperid = self._get_col(col_paperid, tbl)

        paperids = parse_id(paperids)

        sq = tbl.select(col_paperid.in_(paperids))
        df = self.read_sql(sq)
        df = df.set_index(col_paperid.name, drop=False)
        if return_type.lower().startswith('dict'):
            return df.to_dict(orient='index')
        else:
            return df

    def get_single_field_for_paper(self, paperid):
        """Given a paper id, return just one field of study (FOS)

        :returns: dictionary with keys 'Field_of_study_ID', 'Field_of_study_name'

        """
        fosids = self.query_field_of_study_for_paperid(paperid)
        toplevel_counts = self.query_toplevel_fields_for_field_ids(fosids)
        if not toplevel_counts:
            return None
        top_id = toplevel_counts.most_common()[0][0]
        top_name = self.query_field_of_study_name(top_id)
        return {
                'Field_of_study_ID': top_id,
                'Field_of_study_name': top_name
                }

    def get_toplevel_cluster(self, paperid):
        """Given a paper id, return the toplevel infomap cluster

        :paperid: TODO
        :returns: TODO

        """
        tbl = self._get_table('tree')
        col_paperid = self._get_col(self.colname_paperid, tbl)
        col_cluster = self._get_col('cl', tbl)
        paperid = parse_id(paperid)
        sq = tbl.select(col_paperid.in_(paperid))
        sq = sq.with_only_columns([col_cluster])
        r = self.engine.execute(sq).fetchone()
        if r:
            cl = r[0]
            toplevel_cl = cl.split(':')[0]
            return toplevel_cl
        else:
            return 0

    
    def get_EF(self, paperid):
        """Given a paper id, return the Eigenfactor score

        :paperid: TODO
        :returns: TODO

        """
        tbl = self._get_table('rank')
        col_paperid = self._get_col(self.colname_paperid, tbl)
        col_EF = self._get_col('EF', tbl)
        paperid = parse_id(paperid)
        sq = tbl.select(col_paperid.in_(paperid))
        sq = sq.with_only_columns([col_EF])
        return self.engine.execute(sq).scalar()

    def get_author_id_list(self, paperid):
        """Given a paper id, return a list of author IDs

        :paperid: TODO
        :returns: TODO

        """
        tbl = self._get_table(self.tblname_paper_authors)
        col_paperid = self._get_col(self.colname_paperid, tbl)
        col_authorid = self._get_col(self.colname_authorid, tbl)
        paperid = parse_id(paperid)
        sq = tbl.select(col_paperid.in_(paperid))
        sq = sq.with_only_columns([col_authorid])
        r = self.engine.execute(sq)
        return [x[0] for x in r.fetchall()]

    
    def get_paperids_from_authorid(self, authorids,
                            table=None,
                            col_authorid=None,
                            col_paperid=None,
                            return_df=False):
        """Return the paper IDs associated with an author ID or list

        :authorids: author ID or list
        :table: table object or name of table mapping Author IDs to Paper IDs
        :col_authorid: column (or name of column) in the table for Author ID
        :col_paperid: column (or name of column) in the table for Paper ID
        :return_df: return a dataframe rather than a series (default False)
        :returns: either a Series of paper IDs or the full DataFrame of the table

        """
        if table is None:
            table = self.tblname_paper_authors
        if col_authorid is None:
            col_authorid = self.colname_authorid
        if col_paperid is None:
            col_paperid = self.colname_paperid

        tbl = self._get_table(table)
        col_authorid = self._get_col(col_authorid, tbl)
        col_paperid = self._get_col(col_paperid, tbl)

        authorids = parse_id(authorids)

        sq = tbl.select(col_authorid.in_(authorids))
        result = self.read_sql(sq)
        if return_df:
            return result
        if result.empty:
            return pd.Series()
        return result.ix[:, col_paperid.name]

    def get_match_against_clause(self, column_name, match_text, only_where_clause=False):
        """Return sql clauses for MATCH {} AGAINST ({} IN NATURAL LANGUAGE MODE)
        Full-text queries (on fields that have full-text index) are not well-supported in SQLAlchemy
        This gets the text to inject into the where clause, and the select clause (which gives the score)
        See MySQL docs for more info

        Make sure to limit queries using this, or add additional WHERE, or else it will probably return too much

        :column_name: (str) name of the column
        :match_text: full-text search string :only_where_clause: only return the where clause, and not the select clause which gives a score
        :returns: where_clause, select_clause (sqlalchemy TextClause objects)

        """
        where_clause = u"MATCH ({}) AGAINST ('{}' IN NATURAL LANGUAGE MODE)".format(column_name, match_text)
        select_clause = where_clause + u" AS SCORE"
        where_clause = text(where_clause)
        select_clause = text(select_clause)
        if only_where_clause:
            return where_clause
        return where_clause, select_clause

    def normalize_title(self, title):
        """Normalize a paper title
        Make lowercase, and remove punctuation

        :title: (string) paper title
        :returns: (string) normalized paper title

        """
        title_norm = title.strip().lower()
        if isinstance(title_norm, unicode):
            table = {ord(char): None for char in string.punctuation}
            title_norm = title_norm.translate(table)
        else:
            table = string.maketrans("", "")
            title_norm = title_norm.translate(table, string.punctuation)
        return title_norm

    def query_title_with_first_characters(self, title, 
                                table=None, 
                                col_title=None, 
                                nchars=50):
        """Query the database for a paper title

        :title: (string) paper title
        :table: table object or name of table for nodes (papers)
        :col_title: TODO
        :nchars: number of characters
        :returns: TODO

        """
        if table is None:
            table=self.tblname_nodes
        if col_title is None:
            col_title = 'Normalized_paper_title'

        tbl = self._get_table(table)
        col_title = self._get_col(col_title, tbl)

        search_str = u"{}%".format(title[:nchars])
        sq = tbl.select(col_title.like(search_str))
        result = self.read_sql(sq)
        return result

    def match_first_characters(self, title, table=None, col_title=None, normalize=True, too_many_threshold=10):
        if table is None:
            table=self.tblname_nodes
        if col_title is None:
            col_title = 'Normalized_paper_title'

        tbl = self._get_table(table)
        col_title = self._get_col(col_title, tbl)

        if normalize:
            title = self.normalize_title(title)

        nchars = 20
        while nchars < 50:
            result = self.query_title_with_first_characters(title, table, col_title, nchars=nchars)
            if len(result) <= too_many_threshold:
                break
            nchars = nchars + 10
        # if still too many results, skip
        if len(result) > too_many_threshold:
            return pd.DataFrame()
        # if no results, skip
        if result.empty:
            return result
        else:
            # print("match with first character query")
            return result

    def fuzzy_match(self, paper_title, compare_titles):
        import difflib
        seq = difflib.SequenceMatcher()
        ratios = []
        for compare_title in compare_titles:
            seq.set_seqs(paper_title.lower(), compare_title.lower())
            ratios.append(seq.ratio())
        return ratios

    def match_paper_title(self, title, 
                        encoding='utf8',
                        table=None, 
                        col_exactmatch=None,
                        col_fulltextmatch=None, 
                        col_paperid=None, 
                        fuzzy_match_threshold=.85,
                        sphinx_search=True):
        """Attempt to match a paper title with an entry in the database
        First, try an exact match with the first characters of the title.
        Then, try a match against the full text index (this can take a lot longer).
        At each step, use fuzzy matching and make sure any matches are above fuzzy_match_threshold.

        :title: (string) The paper title
        :fuzzy_match_threshold: TODO
        :returns: TODO

        """
        if table is None:
            table=self.tblname_nodes
        if col_exactmatch is None:
            col_exactmatch = 'Normalized_paper_title'
        if col_fulltextmatch is None:
            col_fulltextmatch = self.colname_title
        if col_paperid is None:
            col_paperid = self.colname_paperid

        tbl = self._get_table(table)
        col_exactmatch = self._get_col(col_exactmatch, tbl)
        col_fulltextmatch = self._get_col(col_fulltextmatch, tbl)
        col_paperid = self._get_col(col_paperid, tbl)

        if not isinstance(title, unicode):
            title = title.decode(encoding)

        title_norm = self.normalize_title(title)

        result = self.match_first_characters(title_norm, tbl, col_exactmatch, normalize=False)
        if not result.empty:
            ratios = self.fuzzy_match(title, result[col_fulltextmatch.name].tolist())
            result['ratio'] = ratios
            result = result[result['ratio']>=fuzzy_match_threshold]
            if result.empty:
                return None
            else:
                result = result.sort_values('ratio')
                # return the paper ID
                return result.iloc[0][col_paperid.name]

        # Sphinx search is fast. Try it:
        # TODO refactor this
        if sphinx_search:
            import MySQLdb
            # from sqlalchemy import create_engine
            # import pandas as pd
            sphinx_conf = {
                    'host': "127.0.0.1",
                    'port': 9306,
                    'user': "",
                    'passwd': "",
                    # 'charset': "utf8",
                    'db': ""
                    }
            # conn_str = "mysql://{user}:{password}@{host}:{port}/{db_name}?charset={charset}".format(**sphinx_conf)
            # print(conn_str)
            # sphinx_engine = create_engine(conn_str)
            sphinx_db = MySQLdb.connect(**sphinx_conf)
            terms = title_norm.split()
            terms = ' | '.join([x for x in terms if len(x) > 3])
            sq = "SELECT * FROM sphinx_paper_title_index WHERE MATCH('{}') LIMIT 10".format(terms)
            # r = sphinx_engine.execute(sq)
            # r = pd.read_sql(sq, sphinx_engine)
            # print r
            cursor = sphinx_db.cursor()
            cursor.execute(sq)

            # get paperids
            paperids = [int(x[0]) for x in cursor.fetchall()]
            sphinx_db.close()
            sphinx_matches = self.query_nodes(paperids)
            ratios = self.fuzzy_match(title, sphinx_matches[col_fulltextmatch.name].tolist())
            sphinx_matches['ratio'] = ratios
            sphinx_matches = sphinx_matches[sphinx_matches['ratio']>=fuzzy_match_threshold]
            if sphinx_matches.empty:
                return None
            else:
                sphinx_matches = sphinx_matches.sort_values('ratio')
                # return the paper ID
                return sphinx_matches.iloc[0][col_paperid.name]



