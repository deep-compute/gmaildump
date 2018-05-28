import base64
import json
import time
import threading
import tornado.httpserver
import tornado.ioloop
import tornado.web

from basescript import BaseScript

import util
from messagestore import *
from gmailhistory import GmailHistory

class RequestHandler(tornado.web.RequestHandler):
    '''
    Get's realtime messages through gmail pub/sub webhooks
    #TODO : write proper doc string

    '''
    def post(self):
        data = json.loads(self.request.body)

        log = self.application.log
        log.info('message received', msg=data)

        msg_data = base64.urlsafe_b64decode(str(data['message']['data']))

        if 'historyId' not in msg_data:
            return

        try:
            gmail = GmailCommand().get_gmail_obj()
            gmail.get_new_msg()
        except IOError as err:
            log.exception(err)


class GmailCommand(BaseScript):

    DESC = 'A tool to get the data from gmail and store it in database'

    GMAIL_WATCH_DELAY = 86400

    def _parse_msg_target_arg(self, t):
        '''
        :param t : str
        :rtype : str, dict

        Eg:
        t = 'forwarder=gmaildump.messagestore.SQLiteStore:db_name=gmail_sqlite:table_name=gmail_dump_sqlit'
        return
             path = gmaildump.messagestore.SQLiteStore
             args = {'db_name': 'gmail_sqlite', 'table_name': 'gmail_dump_sqlit'}

        '''

        path, args = t.split(':', 1)
        path = path.split('=')[1]
        args = dict(a.split('=', 1) for a in args.split(':'))
        args['log'] = self.log

        return path, args

    def msg_store(self):
        '''
        :rtype : list

        '''
        targets = []

        for t in self.args.target:
            imp_path, args = self._parse_msg_target_arg(t)
            target_class = util.load_object(imp_path)
            target_obj = target_class(**args)
            targets.append(target_obj)

        return targets

    def watch_gmail(self):
        '''Renewing mailbox watch
        You must re-call watch() at least every 7 days or else you will stop receiving pub/sub updates for the user.
        We recommend calling watch() once per day. The watch() response also has an
        expiration field with the timestamp for the watch expiration.

        :ref : https://developers.google.com/gmail/api/guides/push

        '''
        while True:
            self.get_gmail_obj().watch_gmail()
            time.sleep(self.GMAIL_WATCH_DELAY)

    def listen_realtime(self):
        self.log.info('Running tornodo on the machine')

        app = tornado.web.Application(handlers=[(r'/', RequestHandler)])
        app.log = self.log
        http_server = tornado.httpserver.HTTPServer(app)
        http_server.listen(self.args.tornodo_port)
        tornado.ioloop.IOLoop.instance().start()

    def get_gmail_obj(self):
        targets = self.msg_store()
        gmail = GmailHistory(cred_path=self.args.credentials_path,
                             query=self.args.api_query,
                             topic_name=self.args.sub_topic,
                             file_path=self.args.file_path,
                             status_path=self.args.status_path,
                             targets=targets, log=self.log)
        gmail.authorize()
        return gmail

    def run(self):
        self.get_gmail_obj().start()
        th = threading.Thread(target=self.watch_gmail)
        th.daemon = True
        th.start()
        self.thread_watch_gmail = th
        self.listen_realtime()

    def define_args(self, parser):
        # gmail api arguments
        parser.add_argument('-cred', '--credentials_path',
                            metavar='usr_credentials_path',
                            help='directory path to get the client \
                            secret and credential files for gmail \
                            api authentication')
        parser.add_argument('-gmail_topic', '--sub_topic',
                            metavar='subscription_topic',
                            help='User created topic to receive \
                            webhooks or push notifications from pub/sub')
        parser.add_argument('-query', '--api_query',
                            metavar='api_query', nargs='?',
                            default='',
                            help='format :list:support@deepcompute.com')

        parser.add_argument('-f', '--file_path', metavar='file_path',
                            nargs='?', default=None,
                            help='The path of the directory where you\
                            want to save gmail inbox attachments')

        # diskdict arguments
        parser.add_argument('-status_path', '--status_path',
                           metavar='status_path', default=None,
                           help='File path where the status of gmail \
                           messages needs to be stored.')

        # database arguments
        parser.add_argument('-target', '--target', nargs='+',
               help='format for Mongo: store=<MongoStore-classpath>:db_name=<database-name>:collection_name=<collection-name> \
               format for SQLite: store=<SQLiteStore-classpath>:host=<hostname>:port=<port-number>:db_name=<db-name>:table_name=<table-name>" \
               format for NSQ: store=<NsqStore-classpath>:host=<hostname>:port=<port-number>:topic=<topic-name> \
               format for file: store=<FileStore-classpath>:file_path=<file-path>')

        # tornodo arguments
        parser.add_argument('-tp', '--tornodo_port', metavar='tornodo_port',
                            nargs='?', default=8788,
                            help='port in which tornodo needs to run')


def main():
    GmailCommand().start()
