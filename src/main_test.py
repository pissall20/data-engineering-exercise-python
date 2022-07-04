import os
import sqlite3

from main import *


def test_type_conversion():
    text_json = {'Id': '5', 'OwnerUserId': '2', 'PostTypeId': '2', 'Score': '6', 'ParentId': '1', 'CommentCount': '2',
                 'LastEditorDisplayName': '', 'ContentLIcense': 'CC BY-SA 3.0',
                 'Body': '<p>In short: the community. I developed a Jenkins plugin recently, and supporting Hudson did not even cross my mind (Jenkins has waaay more plugins).</p>\n',
                 'CreationDate': '2017-02-28T16:54:32.700', 'LastActivityDate': '2017-02-28T16:54:32.700'}
    expected_json = {'Id': 5, 'OwnerUserId': 2, 'PostTypeId': 2, 'Score': 6, 'ParentId': 1, 'CommentCount': 2,
                     'LastEditorDisplayName': '', 'ContentLIcense': 'CC BY-SA 3.0',
                     'Body': '<p>In short: the community. I developed a Jenkins plugin recently, and supporting Hudson did not even cross my mind (Jenkins has waaay more plugins).</p>\n',
                     'CreationDate': datetime(2017, 2, 28, 16, 54, 32, 700000),
                     'LastActivityDate': datetime(2017, 2, 28, 16, 54, 32, 700000)}
    assert (type_conversion(text_json) == expected_json)
