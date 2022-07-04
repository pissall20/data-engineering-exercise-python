import json
import os
import sys
from datetime import datetime

from db import *

# The following code is purely illustrative
# try:
#     with open('uncommitted/Posts.json', 'r') as posts_in:
#         print(json.load(posts_in)[0])
# except FileNotFoundError:
#     print("Please download the dataset using 'pipenv run fetch_data'")

POSTS_TABLE_NAME = "posts"
VOTES_TABLE_NAME = "votes"
POST_COLUMNS = ['AnswerCount', 'LastEditorDisplayName', 'Tags', 'OwnerUserId', 'OwnerDisplayName', 'ViewCount',
                'LastActivityDate', 'LastEditDate', 'FavoriteCount', 'Body', 'CommunityOwnedDate', 'CommentCount',
                'Title', 'Id', 'Score', 'ParentId', 'ContentLIcense', 'ClosedDate', 'PostTypeId', 'AcceptedAnswerId',
                'CreationDate', 'LastEditorUserId']
VOTE_COLUMNS = ['BountyAmount', 'CreationDate', 'Id', 'UserId', 'PostId', 'VoteTypeId']


def create_tables(db_cursor):
    posts_query = f"Create Table if not exists {POSTS_TABLE_NAME} "
    votes_query = f"Create Table if not exists {VOTES_TABLE_NAME} "
    posts_schema = {'Id': 'INTEGER PRIMARY KEY', 'OwnerUserId': 'INTEGER', 'LastEditorUserId': 'INTEGER',
                    'PostTypeId': 'INTEGER', 'ParentId': 'INTEGER', 'ClosedDate': 'timestamp',
                    'AcceptedAnswerId': 'INTEGER', 'Score': 'INTEGER', 'ViewCount': 'INTERGER',
                    'AnswerCount': 'INTEGER', 'CommentCount': 'INTEGER', 'LastEditorDisplayName': 'TEXT',
                    'Title': 'TEXT', 'OwnerDisplayName': 'TEXT',
                    'Tags': 'TEXT', 'ContentLIcense': 'TEXT', 'Body': 'TEXT',
                    'FavoriteCount': 'INTEGER', 'CreationDate': 'timestamp', 'CommunityOwnedDate': "timestamp",
                    'LastEditDate': 'timestamp', 'LastActivityDate': 'timestamp'}

    votes_schema = {'Id': 'INTEGER PRIMARY KEY', 'UserId': 'INTEGER', 'PostId': 'INTEGER', 'VoteTypeId': 'INTEGER',
                    'CreationDate': 'timestamp', 'BountyAmount': 'INTEGER'}

    posts_query = posts_query + \
        "(" + ",".join([f"{k} {v}" for (k, v) in posts_schema.items()]) + ")"
    votes_query = votes_query + \
        "(" + ",".join([f"{k} {v}" for (k, v) in votes_schema.items()]) + ")"
    execute_query(db_cursor, posts_query)
    print("Created Posts Table")
    execute_query(db_cursor, votes_query)
    print("Created Votes Table")
    return


def type_conversion(post_or_votes_dict: dict):
    int_columns = ['AnswerCount', 'BountyAmount', 'Id', 'UserId', 'Score', 'OwnerUserId', 'VoteTypeId', 'ParentId',
                   'FavoriteCount', 'ViewCount', 'LastEditorUserId', 'PostId', 'PostTypeId', 'CommentCount',
                   'AcceptedAnswerId']
    ts_columns = ['LastEditDate', 'CreationDate',
                  'ClosedDate', 'LastActivityDate', 'CommunityOwnedDate']

    for k, v in post_or_votes_dict.items():
        if v:
            if k in int_columns:
                post_or_votes_dict[k] = int(v)
            elif k in ts_columns:
                post_or_votes_dict[k] = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
    return post_or_votes_dict


def fix_posts_json(post_dict: dict):
    for col in POST_COLUMNS:
        if col not in post_dict:
            post_dict[col] = None
    post_dict = type_conversion(post_dict)
    return post_dict


def fix_votes_json(vote_dict: dict):
    for col in VOTE_COLUMNS:
        if col not in vote_dict:
            vote_dict[col] = None
    vote_dict = type_conversion(vote_dict)
    return vote_dict


def insert_posts(db_cursor, posts_json_data, verbose=True):
    posts_insert_query = create_insert_query(POSTS_TABLE_NAME, POST_COLUMNS)
    posts_json_data = fix_posts_json(posts_json_data)
    values = [posts_json_data[col] for col in POST_COLUMNS]
    execute_query(db_cursor, posts_insert_query, values)
    if verbose:
        print(f"Added, Post ID: {posts_json_data['Id']}")


def insert_votes(db_cursor, votes_json_data, verbose=True):
    vote_insert_query = create_insert_query(VOTES_TABLE_NAME, VOTE_COLUMNS)
    vote_json_data = fix_votes_json(votes_json_data)
    values = [votes_json_data[col] for col in VOTE_COLUMNS]
    execute_query(db_cursor, vote_insert_query, values)
    if verbose:
        print(f"Added, Vote ID: {vote_json_data['Id']}")


def insert_data(db_cursor, posts_data, votes_data):
    for post in posts_data:
        insert_posts(db_cursor, post, verbose=False)
    for vote in votes_data:
        insert_votes(db_cursor, vote, verbose=False)
    print("Inserted all data")


if __name__ == '__main__':
    if not len(sys.argv) == 3:
        raise ValueError("Wrong number of arguments passed")

    posts_file = sys.argv[1]
    votes_file = sys.argv[2]
    sqlite_db_file = r"warehouse.db"
    if not os.path.exists(sqlite_db_file):
        print(f"Creating database: {sqlite_db_file}")
        create_database(sqlite_db_file)

    db_conn = create_connection(sqlite_db_file)
    cursor = db_conn.cursor()

    create_tables(cursor)

    posts_json = json.load(open(posts_file))
    votes_json = json.load(open(votes_file))

    insert_data(cursor, posts_json, votes_json)
