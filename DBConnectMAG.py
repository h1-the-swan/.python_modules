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

        
