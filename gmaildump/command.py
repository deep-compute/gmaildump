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

    DESC = "Gets realtime messages through gmail pub/sub webhooks"

    def post(self):
        """
        ref: https://developers.google.com/gmail/api/guides/push#receiving_notifications

        """
        data = json.loads(self.request.body)
        msg_data = base64.urlsafe_b64decode(str(data["message"]["data"]))

        if "historyId" not in msg_data:
            return

        try:
            gmail = GmailCommand().get_gmail_obj()
            gmail.get_new_msg()
        except IOError as err:  # TODO: Diskdict error
            log.exception(err)


class GmailCommand(BaseScript):
    DESC = "A tool to get the data from gmail and store it in database"

    def _parse_msg_target_arg(self, t):
        """
        >>> from command import GmailCommand
        >>> obj = GmailCommand()
        >>> obj._parse_msg_target_arg('forwarder=gmaildump.messagestore.SQLiteStore:db_name=gmail_sqlite:table_name=gmail_dump_sqlit')
        ('gmaildump.messagestore.SQLiteStore', {'db_name': 'gmail_sqlite', 'table_name': 'gmail_dump_sqlit'})

        """
        path, args = t.split(":", 1)
        path = path.split("=")[1]
        args = dict(a.split("=", 1) for a in args.split(":"))

        return path, args

    def msg_store(self):
        targets = []

        for t in self.args.target:
            imp_path, args = self._parse_msg_target_arg(t)
            target_class = util.load_object(imp_path)
            target_obj = target_class(**args)
            targets.append(target_obj)

        return targets

    def listen_realtime(self):
        self.log.info("Running tornodo on the machine")

        app = tornado.web.Application(handlers=[(r"/", RequestHandler)])
        http_server = tornado.httpserver.HTTPServer(app)
        http_server.listen(self.args.tornodo - port)
        tornado.ioloop.IOLoop.instance().start()

    def get_gmail_obj(self):
        targets = self.msg_store()
        gmail = GmailHistory(
            cred_path=self.args.credentials - path,
            query=self.args.api - query,
            topic_name=self.args.sub - topic,
            file_path=self.args.file - path,
            status_path=self.args.status - path,
            targets=targets,
            log=self.log,
        )
        gmail.authorize()  # authorizing gmail service in order to make gmail api calls
        return gmail

    def run(self):
        gmail = self.get_gmail_obj()

        # start getting the gmail msgs from users mailbox
        gmail.start()

        # call gmail api watch request every day
        th = threading.Thread(target=gmail.renew_mailbox_watch)
        th.daemon = True
        th.start()
        self.thread_watch_gmail = th

        # listen for real time msgs on tornodo specified port
        self.listen_realtime()

    def define_args(self, parser):
        # gmail api arguments
        parser.add_argument(
            "-cred",
            "--credentials-path",
            required=True,
            help="directory path to get the client \
                            secret and credential files for gmail \
                            api authentication",
        )
        parser.add_argument(
            "-gmail_topic",
            "--sub-topic",
            required=True,
            help="The topic to which \
                            webhooks or push notifications has subscibed from pub/sub",
        )
        parser.add_argument(
            "-query",
            "--api-query",
            nargs="?",
            help="query to get required msgs,\
                            eg: from: support@deepcompute.com\
                            ref:https://support.google.com/mail/answer/7190?hl=en",
        )

        # attachments arguments
        parser.add_argument(
            "-f",
            "--file-path",
            nargs="?",
            help="The path of the directory where user\
                            want to save gmail inbox attachments. By default attachements \
                            will not been stored",
        )

        # diskdict arguments
        parser.add_argument(
            "-status_path",
            "--status-path",
            default="/tmp",
            help="File path where the status of gmail \
                           messages needs to be stored. Default path: /tmp/",
        )

        # database arguments
        parser.add_argument(
            "-target",
            "--target",
            nargs="+",
            help='format for Mongo: store=<MongoStore-classpath>:db_name=<database-name>:collection_name=<collection-name> \
           format for SQLite: store=<SQLiteStore-classpath>:host=<hostname>:port=<port-number>:db_name=<db-name>:table_name=<table-name>" \
           format for NSQ: store=<NsqStore-classpath>:host=<hostname>:port=<port-number>:topic=<topic-name> \
           format for file: store=<FileStore-classpath>:file_path=<file-path>',
        )

        # tornodo arguments
        parser.add_argument(
            "-tp",
            "--tornodo-port",
            nargs="?",
            default=8788,
            help="port in which tornodo needs to run to get realtime msgs\
                            default port: 8788",
        )


def main():
    GmailCommand().start()
