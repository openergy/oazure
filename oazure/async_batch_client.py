import datetime as dt
import base64
import hashlib
import hmac
import asyncio

import aiohttp
from aiohttp.client_exceptions import ClientError

from .snippets.ojson import dumps


class BatchResponseError(Exception):
    def __init__(self, code, message, values):
        self.code = code
        self.message = message
        self.values = values
        super().__init__(
            f"ResponseError: {code}\n{message['value']}\n" + "\n".join([f"{d['key']}: {d['value']}" for d in values])
        )


class AzureBatchClient:
    def __init__(self, account_name, account_key, account_url):
        self.account_name = account_name
        self.account_key = account_key
        self.account_url = account_url.strip("/")
        self.api_version = "2018-12-01.8.0"
        self.session = None

    async def _send(self, verb, path, params, headers, body=None, json=None, retries=3):
        if self.session is None:
            self.session = aiohttp.ClientSession()

        url = self.account_url + path
        response = None
        if body is not None:
            if json is not None:
                raise ValueError("body and json must not be set at the same time")
            headers["Content-Type"] = "application/octet-stream"
        elif json is not None:
            headers["Content-Type"] = "application/json; odata=minimalmetadata; charset=utf-8"
            body = bytes(dumps(json), "utf-8")
        headers["Content-Length"] = str(len(body)) if body else "0"
        headers = self._authenticate(verb, path, params, headers)
        for retry in range(retries):
            try:
                response = await self.session.request(
                    verb,
                    url,
                    params=params,
                    headers=headers,
                    data=body,
                    skip_auto_headers=("Content-Type", "User-Agent", "Content-Length")
                )
            except ClientError:
                if retry == retries - 1:
                    raise
            else:
                break

        if response.status // 100 == 2:
            return response
        else:
            content = await response.json()
            raise BatchResponseError(content["code"], content["message"], content["values"] if "values" in content else [])

    def _authenticate(self, verb, path, params, headers):
        headers["Date"] = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = "\n".join((
                verb,
                headers.get("Content-Encoding", ""),
                headers.get("Content-Language", ""),
                headers.get("Content-Length", ""),
                headers.get("Content-MD5", ""),
                headers.get("Content-Type", ""),
                headers.get("Date"),
                headers.get("If-Modified-Since", ""),
                headers.get("If-Match", ""),
                headers.get("If-None-Match", ""),
                headers.get("If-Unmodified-Since", ""),
                headers.get("Range", ""),
                *sorted([f"{key}:{value}" for key, value in headers.items() if key.startswith("ocp-")]),
                f"/{self.account_name}{path}",
                *sorted([f"{key}:{value}" for key, value in params.items()])
        ))
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers["Authorization"] = f"SharedKey {self.account_name}:{signature}"
        return headers

    async def add_task(
            self,
            job_id,
            task_id,
            command_line,
            environment_settings=None,
            output_files=None,
            timeout=None
    ):
        """

        Parameters
        ----------
        job_id: str
        task_id: str
        command_line: str
        environment_settings: list
        output_files: list
        timeout

        Returns
        -------

        """
        path = f"/jobs/{job_id}/tasks"
        parameters = {
            "api-version": self.api_version
        }
        if timeout is not None:
            parameters["timeout"] = timeout

        headers = {}

        json_body = {
            "id": task_id,
            "commandLine": command_line,
            # for now userIdentity is fixed to pool user and admin, can be modified if needed
            "userIdentity": {
                "autoUser": {
                    "scope": "pool",
                    "elevationLevel": "admin"
                }
            },
            "environmentSettings": [] if environment_settings is None else environment_settings,
            "outputFiles": [] if output_files is None else output_files
        }

        response = await self._send("POST", path, parameters, headers, json=json_body)
        response.release()
        return response

    async def get_task(self, job_id, task_id, timeout=None):
        """
        Parameters
        ----------
        jobd_id: str
        task_id: str

        Returns: dict
        -------
        """
        path = f"/jobs/{job_id}/tasks/{task_id}"

        params = {"api-version": self.api_version}
        if timeout is not None:
            params["timeout"] = timeout

        headers = {}

        response = await self._send("GET", path, params, headers)
        ret = await response.json()
        response.release()

        return ret

    async def clear_job(self, job_id):
        """

        Parameters
        ----------
        job_id: str
        """
        path = f"/jobs/{job_id}/tasks"
        params = {"api-version": self.api_version}
        headers = {}

        response = await self._send("GET", path, params, headers)

        tasks_list = [task['id'] for task in (await response.json())["value"]]
        response.release()

        await asyncio.gather(*[self.delete_task(job_id, pk) for pk in tasks_list])

    async def delete_task(self, job_id, task_id):
        """

        Parameters
        ----------
        job_id: str
        task_id: str
        """
        path = f"/jobs/{job_id}/tasks/{task_id}"
        params = {"api-version": self.api_version}
        headers = {}

        response = await self._send("DELETE", path, params, headers)
        response.release()
