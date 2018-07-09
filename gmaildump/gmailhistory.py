import time
import base64
from multiprocessing.pool import ThreadPool
from copy import deepcopy
from datetime import datetime, timedelta
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

from deeputil import Dummy, AttrDict
from diskdict import DiskDict

DUMMY_LOG = Dummy()


class GmailHistory(object):

    '''
    This is the main class you instantiate to access the Gmail API to get the messages from user's mailbox.
    :ref : https://developers.google.com/gmail/api/v1/reference/

    '''

    MAX_RESULTS = 500                   # gmail api max results
    LABELIDS = ['INBOX']                # labels to which, pub/sub updates are to be pushed
    GMAIL_CREATED_TS = '2004/01/01'     # year in which gmail has introduced
    GMAIL_WATCH_DELAY = 86400           # time in sec to make gmail api watch() request
    SCOPES = 'https://www.googleapis.com/auth/gmail.readonly' # type of permission to access gmail api

    def __init__(self, cred_path=None, topic_name=None,
                 query='', file_path=None, status_path='/tmp/',
                 targets=None, log=DUMMY_LOG):

        self.log = log
        self.cred_path = cred_path
        self.query = query
        self.gmail = None
        self.topic = topic_name
        self.file_path = file_path
        self.dd = DiskDict(status_path + 'disk.dict')
        self.targets = targets
        self._pool = ThreadPool()

    def authorize(self):
        '''

        Gets valid user credentials from the user specified cred path
        if nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is incomplete and throws an authentication failed error or file not found error

        client_secret.json : This is the name of the secret file you download from
                             https://console.developers.google.com/iam-admin/projects

        credentials.json : This is the file that will be created when user has authenticated and
                           will mean you don't have to re-authenticate each time you connect to the API
        '''
        self.log.debug('authorize')

        store = file.Storage('{}credentials.json'.format(self.cred_path))
        creds = store.get()

        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('{}client_secret.json'.format(self.cred_path),
                                                 self.SCOPES)
            creds = tools.run_flow(flow, store)

        # build return gmail service object on authentication
        self.gmail = build('gmail', 'v1', http=creds.authorize(Http()),
                           cache_discovery=False)

        return self.gmail

    def save_files(self, message):
        '''
        This fun helps to store gmail attachments from the given message.

        :calls : GET https://www.googleapis.com/gmail/v1/users/userId/messages/messageId/attachments/id
        :param message : dict

        '''
        self.log.debug('save_file')

        for part in message['payload'].get('parts', ''):

            if not part['filename']:
                continue

            file_id = part['body']['attachmentId']
            file_dic = self.gmail.users().messages().attachments().get(
                userId='me', messageId=message['id'], id=file_id).execute()

            file_data = base64.urlsafe_b64decode(file_dic['data'].encode('UTF-8'))
            path = ''.join([self.file_path, part['filename']])

            with open(path, 'w') as file_obj:
                file_obj.write(file_data)

            self.log.info('attachment saved to', path=path)

    def set_tmp_ts_to_last_msg(self):
        '''

        This fun help to reset last_msg_ts to tmp_ts

        '''
        self.log.debug('set_tmp_ts_to_last_msg')

        self.dd['last_msg_ts'] = self.dd['tmp_ts']
        self.dd.close()

    def renew_mailbox_watch(self):
        '''Renewing mailbox watch

        You must re-call watch() at least every 7 days or else you will stop receiving pub/sub updates.
        We recommend calling watch() once per day. The watch() response also has an
        expiration field with the timestamp for the watch expiration.

        :ref : https://developers.google.com/gmail/api/guides/push

        '''
        while True:
            self.watch_gmail()
            time.sleep(self.GMAIL_WATCH_DELAY)

    def get_new_msg(self):
        '''
        This fun help us to see any changes to the user's mailbox and gives new msgs if they are available.
        Note : startHistoryId - returns Histories(drafts, mail deletions, new mails) after start_history_id.

        :calls : GET https://www.googleapis.com/gmail/v1/users/userId/history

        >>> from mock import Mock, MagicMock
        >>> obj = GmailHistory()
        >>> obj.store_msgs_in_db = Mock()
        >>> obj.set_tmp_ts_to_last_msg = Mock()
        >>> obj.gmail = Mock()
        >>> obj.dd = MagicMock()
        >>> sample_doc = {'history': [{'messagesAdded': [{'message': {'labelIds': ['UNREAD'], 'id': '163861dac0f17c61'}}]}]}
        >>> obj.gmail.users().history().list().execute = Mock(obj.gmail.users().history().list().execute,return_value=sample_doc)
        >>> obj.get_new_msg()
        [{'labelIds': ['UNREAD'], 'id': '163861dac0f17c61'}]

        '''
        self.log.debug('get_new_msg')

        msg_list = []
        new_msg = self.gmail.users().history().list(
            userId='me', startHistoryId=self.dd['historyId']).execute()

        if 'history' not in new_msg:
            return

        for record in new_msg.get('history'):

            if 'messagesAdded' not in record:
                continue

            msg = record.get('messagesAdded')[0]['message']

            if msg.get('labelIds')[0] == 'DRAFT':
                continue

            msg_list.append(msg)

        self.store_msgs_in_db(msg_list)
        self.set_tmp_ts_to_last_msg()

        return msg_list

    def watch_gmail(self):
        '''To recive Push Notifications

        In order to receive notifications from Cloud Pub/Sub topic,
        simply we can call watch() from google api client on the Gmail user mail box.
        :ref : https://developers.google.com/gmail/api/guides/push

        :calls : POST https://www.googleapis.com/gmail/v1/users/userId/watch

        >>> from mock import Mock
        >>> obj = GmailHistory()
        >>> obj.gmail = Mock()
        >>> api_doc = {'historyId':1234,'expiration':1526901631234}
        >>> obj.gmail.users().watch().execute = Mock(obj.gmail.users().watch().execute, return_value=api_doc)
        >>> obj.watch_gmail()
        {'expiration': 1526901631234, 'historyId': 1234}

        '''
        self.log.debug('watch_gmail')

        request = {
            'labelIds': self.LABELIDS,
            'topicName': '{}'.format(self.topic)
        }

        hstry_id = self.gmail.users().watch(userId='me', body=request).execute()

        self.log.info('Gmail_watch_id :', hstryid=hstry_id)

        return hstry_id

    def send_msgs_to_target(self, target, msg):
        '''
        This fun helps to send msg to target database and store it.

        :param target : db_obj
        :param msg : dict

        '''
        self.log.debug('send msgs to tatgets')

        target.insert_msg(msg)

    def write_message(self, msg):
        '''
        This function helps to push msgs to databases in asynchronous manner, if more than one db is specified.

        :param msg: dict

        '''
        self.log.debug('write msgs in db')

        if self.targets:
            fn = self.send_msgs_to_target

            jobs = []
            for t in self.targets:
                jobs.append(self._pool.apply_async(fn, (t, deepcopy(msg))))

            for j in jobs:
                j.wait()

    def change_diskdict_state(self, message):
        '''
        This fun helps us to change the state of diskdict

        :param message : dict

        '''

        # for every msg the last_msg_ts will be replace with new msg internalDate
        self.dd['last_msg_ts'] = message['internalDate']

        if 'frst_msg_ts' not in self.dd.keys() \
                or (self.dd['frst_msg_ts'] <= message['internalDate']):
            self.dd['frst_msg_ts'] = message['internalDate']
            self.dd['historyId'] = message['historyId']

    def store_msgs_in_db(self, msgs_list):
        '''
        Get msg ids from list of messages amd makes an api call with the msg id
        and store in db.

        :params msgs_list : list
        :calls : GET https://www.googleapis.com/gmail/v1/users/userId/messages/id

        '''
        self.log.debug('store_msgs_in_db')

        for msg in msgs_list:

            message = self.gmail.users().messages().get(userId='me',
                                                        id=msg['id']).execute()

            self.write_message(message)
            self.change_diskdict_state(message)

            if self.file_path:
                self.save_files(message)

    def get_default_ts(self):
        '''
        This fun helps to return next day date from today in Y/m/d format

        :rtype: str

        '''
        self.log.debug('get_default_ts')

        return (datetime.now() + timedelta(days=1)).strftime('%Y/%m/%d')

    def get_history(self, before, after=GMAIL_CREATED_TS):
        '''
        Get all the msgs from the user's mailbox with in given dates and store in the db
        Note : Gmail api will consider 'before' : excluded date, 'after' : included date
        Eg: before : 2017/01/01, after : 2017/01/31 then gmail api gives msgs from 2017/01/02 - 2017/01/31

        :ref : https://developers.google.com/gmail/api/guides/filtering
        :calls : GET https://www.googleapis.com/gmail/v1/users/userId/messages

        :param before : string
        :param after : string
        :rtype : list

        >>> from mock import Mock
        >>> obj = GmailHistory()
        >>> obj.gmail = Mock()
        >>> api_doc = {'messages':[{'id':'163861dac0f17c61'},{'id':'1632163b6a84ab94'}]}
        >>> obj.gmail.users().messages().list().execute = Mock(obj.gmail.users().messages().list().execute, return_value=api_doc)
        >>> obj.store_msgs_in_db = Mock()
        >>> obj.get_history('2017/05/10')
        [{'id': '163861dac0f17c61'}, {'id': '1632163b6a84ab94'}]

        '''
        self.log.debug('fun get history')

        query = '{} before:{} after:{}'.format(self.query, before, after)
        response = self.gmail.users().messages().list(
            userId='me', maxResults=self.MAX_RESULTS, q=query).execute()
        msgs = []
        response = AttrDict(response)

        if 'messages' in response:
            msgs.extend(response.messages)
            self.store_msgs_in_db(response.messages)

        while 'nextPageToken' in response:
            page_token = response.nextPageToken
            response = self.gmail.users().messages().list(userId='me',
                       maxResults=self.MAX_RESULTS, q=query,
                       pageToken=page_token).execute()
            response = AttrDict(response)

            if response.resultSizeEstimate is not 0:
                msgs.extend(response.messages)
                self.store_msgs_in_db(response.messages)

        return msgs

    def get_oldest_date(self, ts):
        '''
        This fun helps to get next day date from given timestamp.

        :param ts: str (Unix time stamp)
        :rtype: str

        >>> obj=GmailHistory()
        >>> obj.get_oldest_date('1526901630000')
        '2018/05/22'

        '''
        self.log.debug('get_oldest_date')

        return (datetime.fromtimestamp(
            int(ts[:10])) + timedelta(days=1)).strftime('%Y/%m/%d')

    def get_latest_date(self, ts):
        '''
        This function helps us to get date from given timestamp

        :param ts: str (Unix time stamp)
        :rtype: str

        >>> obj=GmailHistory()
        >>> obj.get_latest_date('1526901630000')
        '2018/05/21'

        '''
        self.log.debug('get_latest_date')

        return (datetime.fromtimestamp(int(ts[:10]))).strftime('%Y/%m/%d')

    def start(self):
        self.log.debug('start')

        # Gets next day date from current date as before_ts in 'yr/m/d' format
        # and check for last_msg_ts key in diskdict file
        before_ts = self.get_default_ts()
        last_msg_ts = self.dd.get('last_msg_ts', 0)

        # If any messages present in diskdict, get the last_msg_ts value  and replace before_ts var with the
        # last_msg_ts with 'yr/m/d' format
        if last_msg_ts:
            before_ts = self.get_oldest_date(last_msg_ts)

        # Get and store the messages from before_ts date to the time gmail has created
        self.get_history(before_ts)
        self.dd['tmp_ts'] = self.dd['last_msg_ts']

        # Recheck for any new messages from the time, execution has happened
        after = self.get_latest_date(self.dd['frst_msg_ts'])
        self.get_history(self.get_default_ts(), after)

        # reset last_msg_ts to temp_ts
        self.set_tmp_ts_to_last_msg()
