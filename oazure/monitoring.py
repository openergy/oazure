import logging
import requests
import datetime
import hashlib
import hmac
import base64
import aiohttp

from .snippets.ojson import dumps

logger = logging.getLogger(__name__)


class LogAnalyticsClient:
    def __init__(self, customer_id, shared_key, log_type):
        self._customer_id = customer_id
        self._shared_key = shared_key
        self.log_type = log_type

    async def send_json_async(self, data):
        uri, body, headers = self._prepare_request(data)
        async with aiohttp.ClientSession() as session:
            response = await session.post(uri, data=body, headers=headers)
        if 200 <= response.status <= 299:
            logger.info('Data accepted by azure log analytics')
            return True
        else:
            logger.warning("Data refused by azure log analytics.", extra=dict(response_code=response.status_code))
            return False

    def send_json(self, data):
        uri, body, headers = self._prepare_request(data)
        response = requests.post(uri, data=body, headers=headers)
        if 200 <= response.status_code <= 299:
            logger.info('Data accepted by azure log analytics')
            return True
        else:
            logger.warning("Data refused by azure log analytics.", extra=dict(response_code=response.status_code))
            return False

    def _prepare_request(self, data):
        body = dumps(data)
        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = self._build_signature(self._customer_id, self._shared_key, rfc1123date, content_length, method, content_type, resource)
        uri = 'https://' + self._customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'
        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': self.log_type,
            'x-ms-date': rfc1123date
        }
        return uri, body, headers

    @staticmethod
    def _build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
        bytes_to_hash = bytes(string_to_hash, encoding='utf-8')
        decoded_key = base64.b64decode(shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest())
        authorization = "SharedKey {}:{}".format(customer_id, encoded_hash.decode('utf-8'))
        return authorization

