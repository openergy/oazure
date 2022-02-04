from azure.storage.blob import BlobPermissions
from rest_framework.response import Response
import datetime as dt


class DownloadResponse(Response):
    def __init__(self, blob_url):
        super(DownloadResponse, self).__init__(status=302, headers=dict(Location=blob_url))


class BlobUrlResponse(Response):
    def __init__(self, blob_url):
        super(BlobUrlResponse, self).__init__(data=dict(
            blob_url=blob_url
        ))


class DRFBlobClient:
    def __init__(self, block_blob_service, sas_expiry):
        self._client = block_blob_service
        self._sas_expiry = sas_expiry

    def download_response(self, container_name, blob_name):
        return DownloadResponse(
            self._client.make_blob_url(container_name, blob_name, protocol="https",
                                       sas_token=self._client.generate_blob_shared_access_signature(
                                           container_name,
                                           blob_name,
                                           permission=BlobPermissions(read=True),
                                           expiry=dt.datetime.now() + dt.timedelta(seconds=self._sas_expiry),
                                           protocol="https"
                                       ))
        )

    def blob_url_response(self, container_name, blob_name, read=False, create=False, add=False, write=False, delete=False):
        return BlobUrlResponse(
            self._client.make_blob_url(container_name, blob_name, protocol="https",
                                       sas_token=self._client.generate_blob_shared_access_signature(
                                           container_name,
                                           blob_name,
                                           permission=BlobPermissions(read=read, create=create, add=add, write=write,
                                                                      delete=delete),
                                           expiry=dt.datetime.now() + dt.timedelta(seconds=self._sas_expiry),
                                           protocol="https"
                                       ))
        )
