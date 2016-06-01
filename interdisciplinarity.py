import sys, os, time, string, json
from datetime import datetime
from collections import Counter
import pandas as pd
import numpy as np
import scipy.io
from scipy.sparse import csr_matrix, coo_matrix
from sklearn.preprocessing import normalize
from citation_db_funcs import parse_id
import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
logger = logging.getLogger(__name__)

class Interdisc(object):

    """Interdisciplinarity object
    :db: database object (e.g. DBConnectMAG)
    :venue_ids: (list or str) If string, will be interpreted as the filename
    for venue IDs. Otherwise, the list of venue IDs
    :venue_matrix: (matrix or str) If string, will be interpreted as th
    filename for the venue matrix. Otherwise, the matrix
    
    """

    def __init__(self, db=None, venue_ids=None, venue_matrix=None):
        self._db = db
        self.venue_ids = None
        self.load_venue_ids(venue_ids)
        self.venue_matrix = None
        self.rownorm = None
        self.colnorm = None
        self.load_venue_matrix(venue_matrix)

    def load_venue_ids(self, venue_ids, sep=','):
        """load venue ids from a file or a list

        :venue_ids: (list or str) If string, will be interpreted as the
        filename for venue IDs. Otherwise, the list of venue IDs
        :sep: delimiter for the file. Default is ','

        """
        if isinstance(venue_ids, basestring):
            with open(venue_ids, 'r') as f:
                read_ids = f.read()
                self.venue_ids = read_ids.strip().split(sep)
        else:
            self.venue_ids = venue_ids

    def load_venue_matrix(self, venue_matrix):
        """load venue matrix (scipy sparse matrix) from a file or a matrix
        Also computes row and column normalized matrices

        :venue_matrix: (matrix or str) If string, will be interpreted as the
        filename for the venue matrix. Otherwise, the matrix

        """
        if isinstance(venue_matrix, basestring):
            start = time.time()
            logger.info("Loading venue matrix...")
            self.venue_matrix = scipy.io.mmread(venue_matrix)
            self.venue_matrix = self.venue_matrix.tocsr()
            end = time.time()
            logger.info("done. took {:.2f} seconds".format(end-start))
        else:
            self.venue_matrix = venue_matrix
        self.rownorm, self.colnorm = self.rowcolnorm(self.venue_matrix)
        
    def jensen_shannon_div(self, dista, distb):
        """Calculate the Jensen Shannon divergence between two distributions

        :dsta, dstb: Normalized distributions (array)
        :returns: Jensen Shannon divergence (scalar)

        """
        log2 = np.log(2)
        # Calculate mean distribution
        distm = (dista + distb) / 2
        aind = np.where(dista>0)[0]
        bind = np.where(distb>0)[0]
        # Calculate KL divergence
        dkla = sum( np.log(dista[aind] / distm[aind]) / log2 * dista[aind] )
        dklb = sum( np.log(distb[bind] / distm[bind]) / log2 * distb[bind] )
        djs = (dkla + dklb) / 2
        return djs

    def integrator(self, outgoingcite, rownorm=None, sparse_matrix=True):
        """Calculate the integrator score from outgoing journal citations

        :outgoingcite: 1D array of outgoing journal citations
        :rownorm: Journal citation matrix normalized by row
        :sparse_matrix: if True, indicates that rownorm is in sparse matrix format
        :returns: integrator score (scalar)

        """
        if rownorm is None:
            rownorm = self.rownorm
        testa = outgoingcite / float(sum(outgoingcite))
        rowind = np.where(testa>0)[0]
        intid = 0
        for j in rowind:
            if sparse_matrix == True:
                intid = intid + self.jensen_shannon_div(testa, rownorm[j].toarray()[0]) * testa[j]
            else:
                intid = intid + self.jensen_shannon_div(testa, rownorm[j]) * testa[j]
        return intid

    def broadcast(self, incomingcite, colnorm=None, sparse_matrix=True):
        """Calculate the broadcast score from incoming journal citations

        :incomingcite: 1D array of incoming journal citations
        :rownorm: Journal citation matrix normalized by column
        :sparse_matrix: if True, indicates that colnorm is in sparse matrix format
        :returns: broadcast score (scalar)

        """
        if colnorm is None:
            colnorm = self.colnorm
        testb = incomingcite / float(sum(incomingcite))
        colind = np.where(testb>0)[0]
        brdid = 0
        for j in colind:
            if sparse_matrix == True:
                brdid = brdid + self.jensen_shannon_div(testb, colnorm[:,j].T.toarray()[0]) * testb[j]
            else:
                brdid = brdid + self.jensen_shannon_div(testb, colnorm[:,j]) * testb[j]
        return brdid

    def normalize_matrix(self, mat, by='row'):
        """Normalize a matrix by row (default) or column (each item divided by row or column sum)

        :mat: citation matrix
        :by: by 'row'(default) or 'column'
        :returns: normalized matrix (if input matrix was sparse, output matrix will also be sparse)

        """
        axis = 1
        if by.lower().startswith('col'):
            axis = 0
        return normalize(mat, norm='l1', axis=axis)

    def rowcolnorm(self, mat):
        """Get the row normalized matrix and the column normalized matrix

        :mat: citation matrix
        :returns: rownorm, colnorm, both matrices (if input matrix was sparse, output matrix will also be sparse)

        """
        rownorm = self.normalize_matrix(mat, by='row')
        colnorm = self.normalize_matrix(mat, by='column')
        return rownorm, colnorm

    def get_venue_counts(self, paperids, colname_venue=None, query_threshold=100000):
        """Get the list of venue counts for paperids in the shape of venue_ids

        :paperids: paperid or list of paperids
        :venue_colnames: list of venue column names (should be a list of length
        1 if only one column). defaults to the database's colname_venue
        property
        :query_threshold: maximum number for query. will do multiple queries if
        this threshold is exceeded.
        :returns: pandas Series of integers

        """
        if colname_venue is None:
            colname_venue = self._db.colname_venue
        if type(paperids) is not pd.Series:
            paperids = parse_id(paperids)
            paperids = pd.Series(paperids)
        lower = 0
        upper = lower + query_threshold
        venue_prefixes = {}
        for i in range(len(colname_venue)):
            venue_prefixes[string.letters[i]] = colname_venue[i]  # 'A', 'B', etc.
        venue_counts = Counter()
        while True:
            subset = paperids.iloc[lower:upper]
            for prefix in venue_prefixes.keys():
                thiscolname = venue_prefixes[prefix]
                r = self._db.query_for_venue_counts(subset, col_venue=thiscolname)
                venue_counts = self._update_counter(venue_counts, r, prefix)
            # query again
            lower = upper
            upper = upper + query_threshold
            if lower > len(paperids):
                break
        counts_df = pd.DataFrame.from_dict(venue_counts, orient='index')
        counts_df = counts_df.reindex(index=self.venue_ids)
        counts_df = counts_df.fillna(value=0)
        if counts_df.empty:
            return None
        else:
            counts_series = counts_df.ix[:, 0].astype(int)
            return counts_series

    def _update_counter(self, venue_counts, r, prefix):
        """update venue counter

        :venue_counts: counter
        :r: sqlalchemy result
        :prefix: str
        :returns: updated counter

        """
        for i in range(r.rowcount):
            row = r.fetchone()
            id_with_prefix = prefix + str(row[1])
            thiscount = row[0]
            venue_counts[id_with_prefix] += thiscount
        return venue_counts

    def get_integrator_score(self, paperids, return_n=False):
        """Get the integrator interdisciplinarity score from a list of paper IDs

        :paperids: paperid or list of paperids
        :return_n: if True, include the number of outcitations considered in the calculation
        :returns: integrator score (float) or (score, num_out_citations) tuple

        """
        Cout = self._db.query_for_citations(paperids, direction='out').astype(str)
        counts_Cout = self.get_venue_counts(Cout)
        if counts_Cout is None:
            score = 0
        else: 
            score = self.integrator(counts_Cout)
        if return_n:
            return (score, len(Cout))
        return score

    def get_broadcast_score(self, paperids, return_n=False):
        """Get the integrator interdisciplinarity score from a list of paper IDs

        :paperids: paperid or list of paperids
        :return_n: if True, include the number of in-citations considered in the calculation
        :returns: broadcast score (float) or (score, num_in_citations) tuple

        """
        Cin = self._db.query_for_citations(paperids, direction='in').astype(str)
        counts_Cin = self.get_venue_counts(Cin)
        if counts_Cin is None:
            score = 0
        else:
            score = self.broadcast(counts_Cin)
        if return_n:
            return (score, len(Cin))
        return score

    def get_scores_from_paperids(self, paperids, return_n=False):
        """Get the integrator and broadcast interdisciplinarity scores from a
        list of paper IDs

        :paperids: paperid or list of paperids
        :return_n: if True, will return two tuples (score, number of in/out citations)
        :returns: integrator_score, broadcast_score (floats)

        """
        integrator_score = self.get_integrator_score(paperids, return_n=return_n)
        broadcast_score = self.get_broadcast_score(paperids, return_n=return_n)
        return integrator_score, broadcast_score

    def get_scores_from_authorids(self, authorids, return_n=False):
        """Get the integrator and broadcast interdisciplinarity scores from a
        list of author IDs (or single author ID)

        :authorids: authorid or list of authorids
        :return_n: if True, will return two tuples (score, number of in/out citations)
        :returns: integrator_score, broadcast_score (floats)

        """
        pids = self._db.get_paperids_from_authorid(authorids)
        integrator_score, broadcast_score = self.get_scores_from_paperids(pids, return_n=return_n)
        return integrator_score, broadcast_score

def load_db():
    from DBConnectMAG import DBConnectMAG
    from settings_db import DATABASE_SETTINGS
    return DBConnectMAG(**DATABASE_SETTINGS['default'])

def main(args):
    if args.paperid:
        try:
            args.paperid = json.loads(args.paperid)
        except ValueError:
            pass
    db = load_db()
    ##interdisc = Interdisc(db, TODO)
    if args.authorid:
        try:
            args.authorid = json.loads(args.authorid)
        except ValueError:
            pass
    ### TODO

if __name__ == "__main__":
    total_start = time.time()
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description="get interdisciplinarity scores")
    parser.add_argument("-p", "--paperid", help='paper ID or (json decodeable) list, e.g. "[2345, 3456, 35677]"')
    parser.add_argument("-a", "--authorid", help='author ID or (json decodeable) list, e.g. "[2345, 3456, 35677]"')
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    if args.paperid and args.authorid:
        logger.error("choose either authorid *or* paperid")
    main(args)
    total_end = time.time()
    logger.info('all finished. total time: {:.2f} seconds'.format(total_end-total_start))
