import os
import sqlite3
from sqlite3.dbapi2 import Connection

from db import *


def test_sqlite3_connection():
    with sqlite3.connect('warehouse.db') as con:
        cursor = con.cursor()
        assert list(cursor.execute('SELECT 1')) == [(1,)]


def test_create_database():
    create_database("test.db")
    assert os.path.exists("test.db")
    os.remove("test.db")


def test_create_connection():
    create_database("test.db")
    conn = create_connection("test.db")
    assert isinstance(conn, Connection)
    os.remove("test.db")


def test_create_insert_query():
    insert_query = create_insert_query("test", ["a", "b", "c"])
    assert (insert_query == 'INSERT INTO test (a,b,c) VALUES (?,?,?);')
