import time
import base64
import hmac
import hashlib
import asyncio
from urllib.parse import quote as url_quote
import requests
import json
import abc

from aiohttp.client import ClientSession
from azure.servicebus import ServiceBusService, Subscription


class ServiceBusApiException(Exception):
    pass


class QueueMessage:
    def __init__(self, body, msg_id=None, lock_token=None):
        self.msg_id = msg_id
        self.lock_token = lock_token
        self.body = body


class ServiceBusHttpRequest:
    def __init__(self):
        self.host = ''  # no https:// or http:// at the beginning of the host name
        self.method = ''  # must be upper case
        self.path = ''
        self.query = []  # list of (key, value) as strings
        self.headers = {}
        self.body = None
        self._url = ''

    def add_content_headers(self):
        if self.method in ['PUT', 'POST', 'MERGE', 'DELETE']:
            self.headers['Content-Length'] = str(len(self.body)) if self.body is not None else '0'

        # if it is not GET or HEAD request, must set content-type.
        if self.method not in ('GET', 'HEAD'):
            for name in self.headers:
                if 'content-type' == name.lower():
                    break
            else:
                self.headers['Content-Type'] = 'application/atom+xml;type=entry;charset=utf-8'

    async def send_request_async(self, key_name, key_value, session : ClientSession):
        self._add_auth(key_name, key_value)
        return await session.request(self.method, self._url, headers=self.headers, data=self.body)

    def send_request_sync(self, key_name, key_value):
        self._add_auth(key_name, key_value)
        return requests.request(self.method, self._url, headers=self.headers, data=self.body)

    def _get_url(self):
        url = "https://" + self.host + ':443' + self.path
        if self.query:
            url += '?' + self.query[0][0] + '=' + self.query[0][1]
            for key, value in self.query[1:]:
                url += '&' + key + '=' + value
        self._url = url

    def _add_auth(self, key_name, key_value):
        if self._url == '':
            self._get_url()
        expiry = str(round(time.time() + 300))
        to_sign = url_quote(self._url, '').lower() + '\n' + expiry

        signature = url_quote(self._sign_string(key_value, to_sign), '')

        auth = 'SharedAccessSignature sig={0}&se={1}&skn={2}&sr={3}'.format(
            signature,
            expiry,
            key_name,
            url_quote(self._url, '').lower()
        )
        self.headers['Authorization'] = auth

    @staticmethod
    def _sign_string(key, string_to_sign):
        key = key.encode('utf-8')
        string_to_sign = string_to_sign.encode('utf-8')
        signed_hmac_sha256 = hmac.HMAC(key, string_to_sign, hashlib.sha256)
        digest = signed_hmac_sha256.digest()
        encoded_digest = base64.b64encode(digest)
        return encoded_digest


class ServiceBusConsumerApi:

    def __init__(self, service_namespace, key_name, key_value, session=None):
        self.service_namespace = service_namespace
        self._key_name = key_name
        self._key_value = key_value
        self._session = session  # if session is None, cannot call the async methods

    async def recv_queue_message_async(self, peek_lock=True, stop_event=None, timeout=None):
        """
        Gets a message from the queue
        If peek_lock is True : a lock is set on the message, when we are done treating the message we should
                               delete/unlock it (at least once delivery)
        If peek_lock is False : the message is returned and deleted (at most once delivery)
        https://docs.microsoft.com/en-us/rest/api/servicebus/peek-lock-message-non-destructive-read
        https://docs.microsoft.com/en-us/rest/api/servicebus/receive-and-delete-message-destructive-read
        :returns messageID, messageContent
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST' if peek_lock else 'DELETE'
        request.path = self._get_api_path() + '/messages/head'
        request.query = [('timeout', str(CONF.azurebus_request_timeout))]
        request.add_content_headers()

        timeout_task = asyncio.get_event_loop().create_task(asyncio.sleep(timeout)) if timeout is not None else None
        stop_event_task = asyncio.get_event_loop().create_task(stop_event.wait()) if stop_event is not None else None
        request_task = asyncio.get_event_loop().create_task(
            request.send_request_async(self._key_name, self._key_value, self._session)
        )
        while True:
            done, pending = await asyncio.wait(
                [request_task] +
                ([] if timeout_task is None else [timeout_task]) +
                ([] if stop_event_task is None else [stop_event_task]),
                return_when=asyncio.FIRST_COMPLETED
            )
            if request_task in done:
                async with request_task.result() as rep:
                    if rep.status == 201:
                        for task in pending:
                            task.cancel()
                        return QueueMessage(
                            body=await rep.read(),
                            msg_id=json.loads(rep.headers['BrokerProperties'])['MessageId'],
                            lock_token=json.loads(rep.headers['BrokerProperties'])['LockToken'] if peek_lock else None
                        )
                    elif rep.status == 204:
                        # the queue is empty, try again and wait more
                        request_task = asyncio.get_event_loop().create_task(
                            request.send_request_async(self._key_name, self._key_value, self._session))
                        continue
                    else:
                        content = await rep.read()
                        for task in pending:
                            task.cancel()
                        raise ServiceBusApiException(content.decode('utf-8'))
            elif timeout_task in done:
                for task in pending:
                    task.cancel()
                raise TimeoutError("Timeout exceeded: %s seconds." % timeout)
            elif stop_event_task in done:
                for task in pending:
                    task.cancel()
                break
            else:
                for task in pending:
                    task.cancel()
                raise ServiceBusApiException

    def recv_queue_message_sync(self, peek_lock=True, timeout=None):
        """
        Gets a message from the queue
        If peek_lock is True : a lock is set on the message, when we are done treating the message we should
                               delete/unlock it (at least once delivery)
        If peek_lock is False : the message is returned and deleted (at most once delivery)
        https://docs.microsoft.com/en-us/rest/api/servicebus/peek-lock-message-non-destructive-read
        https://docs.microsoft.com/en-us/rest/api/servicebus/receive-and-delete-message-destructive-read
        :returns messageID, messageContent
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST'
        request.path = self._get_api_path() + '/messages/head'

        time_spent = 0

        while time_spent < timeout:
            request_timeout = min(CONF.azurebus_request_timeout, timeout-time_spent)
            request.query = [('timeout', str(request_timeout))]
            request.add_content_headers()
            rep = request.send_request_sync(self._key_name, self._key_value)
            if rep.status_code == 201:
                return QueueMessage(
                    body=rep.content,
                    msg_id = json.loads(rep.headers['BrokerProperties'])['MessageId'],
                    lock_token = json.loads(rep.headers['BrokerProperties'])['LockToken'] if peek_lock else None
                )
            elif rep.status_code == 204:
                # the queue is empty for now, try again
                time_spent += request_timeout
            else:
                content = rep.content
                raise ServiceBusApiException(content.decode('utf-8'))

        raise TimeoutError("Timeout exceeded: %s seconds." % timeout)

    async def unlock_message_async(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/unlock-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'PUT'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = await request.send_request_async(self._key_name, self._key_value, self._session)

        if rep.status != 200:
            raise ServiceBusApiException((await rep.read()).decode('utf-8'))

    def unlock_message_sync(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/unlock-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'PUT'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = request.send_request_sync(self._key_name, self._key_value)

        if rep.status_code != 200:
            raise ServiceBusApiException(rep.content.decode('utf-8'))

    async def delete_message_async(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/delete-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'DELETE'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = await request.send_request_async(self._key_name, self._key_value, self._session)

        if rep.status != 200:
            raise ServiceBusApiException((await rep.read()).decode('utf-8'))

    def delete_message_sync(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/delete-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'DELETE'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = request.send_request_sync(self._key_name, self._key_value)

        if rep.status_code != 200:
            raise ServiceBusApiException(rep.content.decode('utf-8'))

    async def renew_message_lock_async(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/renew-lock-for-a-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = await request.send_request_async(self._key_name, self._key_value, self._session)

        if rep.status != 200:
            raise ServiceBusApiException((await rep.read()).decode('utf-8'))

    def renew_message_lock_sync(self, message_id, lock_token):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/renew-lock-for-a-message
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST'
        request.path = self._get_api_path() + '/messages' + '/' + message_id + '/' + lock_token
        request.add_content_headers()

        rep = request.send_request_sync(self._key_name, self._key_value)

        if rep.status_code != 200:
            raise ServiceBusApiException(rep.content.decode('utf-8'))

    @abc.abstractclassmethod
    def _get_api_path(self):
        pass


class ServiceBusProducerApi:

    def __init__(self, service_namespace, key_name, key_value, session=None):
        self.service_namespace = service_namespace
        self._key_name = key_name
        self._key_value = key_value
        self._session = session  # if session is None, cannot call the async methods

    async def send_message_async(self, message):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST'
        request.path = self._get_api_path() + '/messages'
        request.body = message
        request.add_content_headers()

        rep = await request.send_request_async(self._key_name, self._key_value, self._session)

        if rep.status != 201:
            raise ServiceBusApiException((await rep.read()).decode('utf-8'))

    def send_message_sync(self, message):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue
        """
        request = ServiceBusHttpRequest()
        request.host = self.service_namespace + '.servicebus.windows.net'
        request.method = 'POST'
        request.path = self._get_api_path() + '/messages'
        request.body = message
        request.add_content_headers()

        rep = request.send_request_sync(self._key_name, self._key_value)

        if rep.status_code != 201:
            raise ServiceBusApiException(rep.content.decode('utf-8'))

    @abc.abstractclassmethod
    def _get_api_path(self):
        pass


class ServiceBusConsumerQueueApi(ServiceBusConsumerApi):
    def __init__(self, queue_name, service_namespace, key_name, key_value, session=None):
        self.queue_name = queue_name
        super().__init__(service_namespace, key_name, key_value, session)

    def _get_api_path(self):
        return '/' + str(self.queue_name)


class ServiceBusProducerQueueApi(ServiceBusProducerApi):
    def __init__(self, queue_name, service_namespace, key_name, key_value, session=None):
        self.queue_name = queue_name
        super().__init__(service_namespace, key_name, key_value, session)

    def _get_api_path(self):
        return '/' + str(self.queue_name)


class ServiceBusProducerTopicApi(ServiceBusProducerApi):
    def __init__(self, topic_name, service_namespace, key_name, key_value, session=None):
        self.topic_name = topic_name
        super().__init__(service_namespace, key_name, key_value, session)

    def _get_api_path(self):
        return '/' + str(self.topic_name)


class ServiceBusConsumerTopicApi(ServiceBusConsumerApi):
    def __init__(self, topic_name, subscription_name, service_namespace, key_name, key_value, session=None):
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self._azure_lib_api = ServiceBusService(
            service_namespace=service_namespace,
            shared_access_key_value=key_value,
            shared_access_key_name=key_name
        )
        super().__init__(service_namespace, key_name, key_value, session)

    def _get_api_path(self):
        return '/' + str(self.topic_name) + '/subscriptions/' + self.subscription_name


    """
    For the next to functions, we use azure library api as only synchronous calls are needed, the doc is no clear and
    there is a lot of xml involved.
    It might be a good idea to re-implement this in the future.
    """

    def create_subscription(
            self,
            lock_duration=None,
            requires_session=None,
            default_message_time_to_live=None,
            dead_lettering_on_message_expiration=None,
            dead_lettering_on_filter_evaluation_exceptions=None,
            enable_batched_operations=None,
            max_delivery_count=None,
            message_count=None
    ):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/create-subscription
        """
        self._azure_lib_api.create_subscription(
            self.topic_name,
            self.subscription_name,
            fail_on_exist=True,
            subscription=Subscription(
                lock_duration=lock_duration,
                requires_session=requires_session,
                default_message_time_to_live=default_message_time_to_live,
                dead_lettering_on_message_expiration=dead_lettering_on_message_expiration,
                dead_lettering_on_filter_evaluation_exceptions=dead_lettering_on_filter_evaluation_exceptions,
                enable_batched_operations=enable_batched_operations,
                max_delivery_count=max_delivery_count,
                message_count=message_count
            ))

    def delete_subscription(self):
        """
        https://docs.microsoft.com/en-us/rest/api/servicebus/delete-subscription
        """
        self._azure_lib_api.delete_subscription(
            topic_name=self.topic_name,
            subscription_name=self.subscription_name,
            fail_not_exist=True
        )
