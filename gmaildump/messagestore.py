import hashlib
import json

import gnsq
import sqlite3
from deeputil import Dummy
from diskdict import DiskDict
from pymongo import MongoClient, ASCENDING, DESCENDING

DUMMY_LOG = Dummy


class SQLiteStore(object):

    def __init__(self, db_name="Gmail", table_name="gmail_dump", log=DUMMY_LOG):
        self.db_name = db_name
        self.table_name = table_name
        self.log = log

        self.con = sqlite3.connect(db_name, check_same_thread=False, isolation_level=None)
        self.db = self.con.cursor()
        self.db.execute(
            "CREATE TABLE if not exists '%s'(key text UNIQUE, message text)" %
            (self.table_name))

    def insert_msg(self, record):
        self.log.info('Msg inserting in sqlite store', record=record['id'])

        try:
            self.db.execute("INSERT INTO {t} VALUES (?, ?)".format(
                t=self.table_name), (record['id'], json.dumps(record)))
        except Exception as e:
            self.log.exception(e)


class FileStore(object):

    def __init__(self, file_path=None, log=DUMMY_LOG):
        self.p = file_path
        self.log = log

    def insert_msg(self, msg):
        self.log.info('Msg inserting in file store', record=record['id'])
        with open(self.p, 'a') as _file:
            _file.write(msg)


class NsqStore(object):

    def __init__(self, topic, host="localhost", port="4151", log=DUMMY_LOG):
        self.topic = topic
        self.log = log
        self.host = host
        self.http_port = port
        self.connection = gnsq.Nsqd(
            address=self.host, http_port=self.http_port)

    def insert_msg(self, record):
        self.connection.publish(self.topic, json.dumps(record))
        self.log.info('msg inserted in nsq store', record=record['id'])


class MongoStore(object):

    def __init__(self, db_name, collection_name, log=DUMMY_LOG):
        self.db_name = db_name
        self.collection_name = collection_name
        self.log = log
        self.client = MongoClient()
        self.db = self.client[self.db_name][self.collection_name]

    def insert_msg(self, msg):
        self.log.info('Msg inserted in monog db', msg_id=msg['id'])

        try:
            self.db.update({'id': msg['id']}, msg, upsert=True)
        except Exception as e:
            self.log.exception(e)
