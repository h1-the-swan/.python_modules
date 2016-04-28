import os

# possible keys: 'protocol', 'host_name', 'user_name', 'password', 'encoding', 'module_path', 'db_name'
DATABASE_SETTINGS = {
        'default': {
            'protocol': 'mysql+pymysql',
            'host_name': '127.0.0.1',
            'user_name': os.environ['MYSQL_USER'],
            'password': os.environ['MYSQL_PWD'],
            'encoding': 'utf8',
            'module_path': None
            }
        }

