import hashlib, hmac, base64
import datetime as dt
import xml.etree.ElementTree as ET

from aiohttp.client_exceptions import ClientError

# TODO : reorganize to avoid having the same code everywhere


class AzureBlobStorageAsyncError(Exception):
    pass


class AzureBlobStorageResourceNotFound(AzureBlobStorageAsyncError):
    pass


class AzureBlobStorageAlreadyLeased(AzureBlobStorageAsyncError):
    pass


class AzureBlobStorageAlreadyReleased(AzureBlobStorageAsyncError):
    pass


class AzureBlobStorageLockedFile(AzureBlobStorageAsyncError):
    pass


class AsyncBlobAPI:
    
    def __init__(
            self,
            account_name,
            account_key,
    ):
        self.account_name = account_name
        self.account_key = account_key
        self.storage_type = 'blob'
        self.api_version = '2016-05-31'
    
    async def get_blob(self, container_name, blob_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' + self.api_version +\
                         '\n/' + self.account_name + '/' + container_name + '/' + blob_name
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name

        retry = 0
        while True:
            try:
                async with session.request("get", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 200:
                        return content
                    elif response.status == 404:
                        raise AzureBlobStorageResourceNotFound(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
                    else:
                        retry += 1
                        if retry > 2:
                            raise AzureBlobStorageAsyncError(
                                '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise AzureBlobStorageAsyncError(
                                '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))

    async def write_blob(self, package, container_name, blob_name, session, lock_id=None, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        # Think to add all the headers, including application/octet-stream for aiohttp
        string_to_sign = 'PUT\n\n\n' + (str(len(package)) if len(package) != 0 else '') +\
                         '\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-blob-type:BlockBlob\nx-ms-date:' + date
        if lock_id is not None:
            string_to_sign += '\nx-ms-lease-id:' + lock_id
        string_to_sign += '\nx-ms-version:' + self.api_version + '\n/' + self.account_name + '/' + container_name +\
                          '/' + blob_name
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "x-ms-blob-type": "BlockBlob",
            "Authorization": "SharedKey " + self.account_name + ":" + signature,
            "Content-Length": str(len(package))
        }
        if lock_id is not None:
            headers['x-ms-lease-id'] = lock_id

        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name

        retry = 0
        while True:
            try:
                async with session.put(url, data=package, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 201:
                        return
                    elif response.status == 412:
                        raise AzureBlobStorageLockedFile(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content),
                                                                             container_name, blob_name))
                    else:
                        retry += 1
                        if retry > 2:
                            raise AzureBlobStorageAsyncError(
                                '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content),
                                                                                 container_name, blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise
    
    async def get_blob_to_text(self, container_name, blob_name, session, encoding='utf-8', timeout=None):
        bytes = await self.get_blob(container_name, blob_name, session, timeout=timeout)
        return bytes.decode(encoding)

    async def write_blob_from_text(
            self,
            text,
            container_name,
            blob_name,
            session,
            encoding='utf-8',
            lock_id=None,
            timeout=None
    ):
        package = bytes(text, encoding)
        await self.write_blob(package, container_name, blob_name, session, lock_id=lock_id, timeout=timeout)

    async def delete_container(self, container_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'DELETE\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' + self.api_version +\
                         '\n/' + self.account_name + '/' + container_name + '\nrestype:container'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name +\
              '?restype=container'

        retry = 0
        while True:
            try:
                async with session.delete(url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 202:
                        return
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}'.format(self.parse_error_code(content), container_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def create_container(self, container_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' +\
                         self.api_version + '\n/' + \
                         self.account_name + '/' + container_name + '\nrestype:container'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" +\
              container_name + '?restype=container'

        retry = 0
        while True:
            try:
                async with session.put(url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 201:
                        return
                    else:
                        error = self.parse_error_code(content)
                        if error == 'ContainerAlreadyExists':
                            raise FileExistsError('{}\nContainer name : {}'.format(error, container_name))
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}'.format(error, container_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def delete_blob(self, container_name, blob_name, session, lock_id=None, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "x-ms-delete-snapshots": 'include'
        }
        if lock_id is not None:
            headers['x-ms-lease-id'] = lock_id
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + '/' +\
              blob_name

        string_to_sign = 'DELETE\n\n\n\n\n\n\n\n\n\n\n'
        for key, val in sorted(headers.items()):
            string_to_sign += '\n' + key + ':' + val
        string_to_sign += '\n/' + self.account_name + '/' + container_name + '/' + blob_name

        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')

        headers["Authorization"] = "SharedKey " + self.account_name + ":" + signature

        retry = 0
        while True:
            try:
                async with session.delete(url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 202:
                        return
                    elif response.status == 404:
                        raise AzureBlobStorageResourceNotFound('{}\nContainer name : {}\nBlob name : {}'.format(
                            self.parse_error_code(content), container_name, blob_name))
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def list_blobs(self, container_name, session, marker=None, maxresults=None, prefix=None, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' + self.api_version +\
                         '\n/' + self.account_name + '/' + container_name + '\ncomp:list'
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" +\
              container_name + '?restype=container&comp=list'
        if marker is not None :
            url += '&marker=' + marker
            string_to_sign += '\nmarker:' + marker
        if maxresults is not None :
            url += '&maxresults=' + str(maxresults)
            string_to_sign += '\nmaxresults:' + str(maxresults)
        if prefix is not None :
            url += '&prefix=' + prefix
            string_to_sign += '\nprefix:' + prefix
        string_to_sign += '\nrestype:container'

        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }

        retry = 0
        while True:
            try:
                async with session.request("get", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status != 200:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}'.format(self.parse_error_code(content), container_name))
                    break
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

        list_element = ET.fromstring(content.decode('utf-8'))
        next_marker = list_element.findtext('NextMarker')
        blobs_element = list_element.find('Blobs')
        names_list = []
        for blob_element in blobs_element.findall('Blob'):
            names_list.append(blob_element.findtext('Name'))
        return next_marker, names_list

    # Takes 2.1 seconds for a 5000 blob container
    async def container_size(self, container_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' + self.api_version +\
                         '\n/' + self.account_name + '/' + container_name + '\ncomp:list\nrestype:container'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" +\
              container_name + '?restype=container&comp=list'

        retry = 0
        while True:
            try:
                async with session.request("get", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status != 200:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}'.format(self.parse_error_code(content), container_name))
                    xml_ans = await response.read()
                    break
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

        list_element = ET.fromstring(content.decode('utf-8'))
        blobs_element = list_element.find('Blobs')
        size = 0
        if blobs_element is not None:
            for blob_element in blobs_element.findall('Blob'):
                properties_element = blob_element.find('Properties')
                size += int(properties_element.findtext('Content-Length'))
        return size

    async def get_blob_size(self, container_name, blob_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'HEAD\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:' + date + '\nx-ms-version:' + self.api_version +\
                         '\n/' + self.account_name + '/' + container_name + '/' + blob_name
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name

        retry = 0
        while True:
            try:
                async with session.request("head", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 200:
                        return int(response.headers['content-length'])
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def acquire_lease(self, container_name, blob_name, lease_duration, session, timeout=None):
        assert 60 >= lease_duration >= 15, 'incorrect lease duration, should be between 15 and 60 seconds'
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-date:' + date + \
                         '\nx-ms-lease-action:acquire\nx-ms-lease-duration:' + str(lease_duration) +\
                         '\nx-ms-version:' + self.api_version + '\n/' + self.account_name + '/' + container_name +\
                         '/' + blob_name + '\ncomp:lease'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature,
            "x-ms-lease-action": "acquire",
            "x-ms-lease-duration": str(lease_duration)
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name + '?comp=lease'

        retry = 0
        while True:
            try:
                async with session.request("put", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 201:
                        return response.headers['X-Ms-Lease-Id']
                    elif response.status == 404:
                        raise AzureBlobStorageResourceNotFound('{}\nContainer name : {}\nBlob name : {}'.format(
                            'The requested blob was not found',
                            container_name,
                            blob_name
                        ))
                    elif response.status == 409:
                        # blob is already leased
                        raise AzureBlobStorageAlreadyLeased('{}\nContainer name : {}\nBlob name : {}'.format(
                            'The blob is already leased',
                            container_name,
                            blob_name
                        ))
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def release_lease(self, container_name, blob_name, lease_id, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-date:' + date + \
                         '\nx-ms-lease-action:release\nx-ms-lease-id:' + lease_id + '\nx-ms-version:' + \
                         self.api_version + '\n/' + self.account_name + '/' + container_name + '/' + blob_name + \
                         '\ncomp:lease'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature,
            "x-ms-lease-action": "release",
            "x-ms-lease-id": lease_id
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name + '?comp=lease'

        retry = 0
        while True:
            try:
                async with session.request("put", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 200:
                        return
                    elif response.status == 404:
                        raise AzureBlobStorageResourceNotFound('{}\nContainer name : {}\nBlob name : {}'.format(
                            self.parse_error_code(content), container_name, blob_name))
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def renew_lease(self, container_name, blob_name, lease_id, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = 'PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-date:' + date + \
                         '\nx-ms-lease-action:renew\nx-ms-lease-id:' + lease_id + '\nx-ms-version:' + \
                         self.api_version + '\n/' + self.account_name + '/' + container_name + '/' + blob_name + \
                         '\ncomp:lease'
        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "Authorization": "SharedKey " + self.account_name + ":" + signature,
            "x-ms-lease-action": "renew",
            "x-ms-lease-id": lease_id
        }
        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + container_name + "/" +\
              blob_name + '?comp=lease'

        retry = 0
        while True:
            try:
                async with session.request("put", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 200:
                        return
                    elif response.status == 409:
                        # blob is already leased
                        raise AzureBlobStorageAlreadyLeased('The blob is already leased')
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content), container_name,
                                                                             blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise

    async def copy_blob(self, source_container_name, source_blob_name, dest_container_name, dest_blob_name, session, timeout=None):
        date = dt.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {
            "x-ms-date": date,
            "x-ms-version": self.api_version,
            "x-ms-copy-source": "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" +
                                source_container_name + "/" + source_blob_name + '?comp=lease',
        }
        string_to_sign = 'PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n'
        for key, val in sorted(headers.items()):
            string_to_sign += '\n' + key + ':' + val
        string_to_sign += '\n/' + self.account_name + '/' + dest_container_name + '/' + dest_blob_name

        signature = base64.b64encode(hmac.new(base64.b64decode(self.account_key), string_to_sign.encode('utf8'),
                                              digestmod=hashlib.sha256).digest()).decode('utf-8')
        headers["Authorization"] = "SharedKey " + self.account_name + ":" + signature

        url = "https://" + self.account_name + "." + self.storage_type + ".core.windows.net/" + dest_container_name + \
              "/" + dest_blob_name

        retry = 0
        while True:
            try:
                async with session.request("put", url, headers=headers, timeout=timeout) as response:
                    content = await response.read()
                    if response.status == 202:
                        return response.headers['x-ms-copy-status'], response.headers['x-ms-copy-id']
                    else:
                        raise AzureBlobStorageAsyncError(
                            '{}\nContainer name : {}\nBlob name : {}'.format(self.parse_error_code(content),
                                                                             dest_container_name, dest_blob_name))
            except ClientError:
                retry += 1
                if retry > 2:
                    raise


    @staticmethod
    def parse_error_code(error):
        error_str = error.decode()
        if not error_str:
            return ""
        root = ET.fromstring(error_str)
        error_element = root.find('Error')
        return root.findtext('Code')






