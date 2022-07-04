import sqlite3
from sqlite3 import Error, IntegrityError


def create_database(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_connection(db_file):
    conn = sqlite3.connect(db_file,
                           # detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                           )
    return conn


def execute_query(db_cursor, query, values=None):
    try:
        if values:
            db_cursor.execute(query, values)
        else:
            db_cursor.execute(query)
    except IntegrityError:
        pass


def create_insert_query(table, columns):
    columns_as_string = "(" + ",".join(columns) + ")"
    insert_query = f"""INSERT INTO {table} {columns_as_string} VALUES ({('?,' * len(columns))[:-1]});"""
    return insert_query
