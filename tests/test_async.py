# import asyncio
# import aiohttp
# import uuid
#
# from oazure.snippets.testing import AsyncTest
# from oazure.azure import AsyncBlobAPI, AzureBlobStorageResourceNotFound as ResourceNotFound
# from azure.storage.blob import BlockBlobService
#
# from .credentials import account_name, account_key
#
#
# class AzureAsyncBlobOperationsTest(AsyncTest):
#     storage_type = "blob"
#     api_version = "2016-05-31"
#     azure_blob_api = BlockBlobService(account_name, account_key)
#     async_blob_api = AsyncBlobAPI(account_name, account_key)
#     container_name = str(uuid.uuid4())
#     blob_name = str(uuid.uuid4())
#     blob_content = uuid.uuid4()
#
#     @classmethod
#     def setUpClass(cls):
#         cls.azure_blob_api.create_container(cls.container_name)
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.azure_blob_api.delete_container(cls.container_name)
#
#     def setUp(self):
#         pass
#
#     def tearDown(self):
#         if self.azure_blob_api.exists(self.container_name, self.blob_name):
#             self.azure_blob_api.delete_blob(self.container_name,self.blob_name)
#
#     def test_get_blob(self):
#         async def f():
#             self.azure_blob_api.create_blob_from_bytes(self.container_name, self.blob_name, self.blob_content.bytes)
#             async with aiohttp.ClientSession() as session:
#                 async_blob_content = await self.async_blob_api.get_blob(self.container_name, self.blob_name,
#                                                                                 session)
#             self.assertEqual(self.blob_content.bytes, async_blob_content)
#         self.async_run(f)
#
#     def test_get_blob_to_text(self):
#         async def f():
#             self.azure_blob_api.create_blob_from_text(self.container_name, self.blob_name, str(self.blob_content))
#             async with aiohttp.ClientSession() as session:
#                 async_blob_content = await self.async_blob_api.get_blob_to_text(self.container_name, self.blob_name,
#                                                                                 session)
#             self.assertEqual(str(self.blob_content), async_blob_content)
#         self.async_run(f)
#
#     def test_write_blob(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.write_blob(self.blob_content.bytes, self.container_name, self.blob_name, session)
#             sync_blob_content = self.azure_blob_api.get_blob_to_bytes(self.container_name, self.blob_name).content
#             self.assertEqual(sync_blob_content, self.blob_content.bytes)
#         self.async_run(f)
#
#     def test_write_blob_from_text(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.write_blob_from_text(str(self.blob_content), self.container_name, self.blob_name, session)
#             sync_blob_content = self.azure_blob_api.get_blob_to_text(self.container_name, self.blob_name).content
#             self.assertEqual(sync_blob_content, str(self.blob_content))
#         self.async_run(f)
#
#     def test_delete_blob(self):
#         async def f():
#             self.azure_blob_api.create_blob_from_bytes(self.container_name, self.blob_name, self.blob_content.bytes)
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.delete_blob(self.container_name, self.blob_name, session)
#             self.assertFalse(self.azure_blob_api.exists(self.container_name, self.blob_name))
#         self.async_run(f)
#
#     def test_delete_blob_with_snapshot(self):
#         async def f():
#             self.azure_blob_api.create_blob_from_bytes(self.container_name, self.blob_name, self.blob_content.bytes)
#             self.azure_blob_api.snapshot_blob(self.container_name, self.blob_name)
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.delete_blob(self.container_name, self.blob_name, session)
#             self.assertFalse(self.azure_blob_api.exists(self.container_name, self.blob_name))
#         self.async_run(f)
#
#     def test_get_blob_size(self):
#         async def f():
#             self.azure_blob_api.create_blob_from_bytes(self.container_name, self.blob_name, self.blob_content.bytes)
#             async with aiohttp.ClientSession() as session:
#                 size = await self.async_blob_api.get_blob_size(self.container_name, self.blob_name, session)
#             self.assertEqual(size, 16)
#         self.async_run(f)
#
#     def test_blob_not_found(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 with self.assertRaises(ResourceNotFound):
#                     await self.async_blob_api.get_blob(str(uuid.uuid4()), self.blob_name, session)
#                 with self.assertRaises(ResourceNotFound):
#                     await self.async_blob_api.get_blob(self.container_name, str(uuid.uuid4()), session)
#         self.async_run(f)
#
#
# class AzureAsyncContainerOperationsTest(AsyncTest):
#     storage_type = "blob"
#     api_version = "2016-05-31"
#     azure_blob_api = BlockBlobService(account_name, account_key)
#     async_blob_api = AsyncBlobAPI(account_name, account_key)
#     container_name = ""
#
#     def setUp(self):
#         self.container_name = str(uuid.uuid4())
#
#     def tearDown(self):
#         if self.azure_blob_api.exists(self.container_name):
#             self.azure_blob_api.delete_container(self.container_name)
#
#     def test_delete_container(self):
#         async def f():
#             self.azure_blob_api.create_container(self.container_name)
#             asyncio.sleep(1)
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.delete_container(self.container_name, session)
#             exists = self.azure_blob_api.exists(self.container_name)
#             self.assertFalse(exists)
#         self.async_run(f)
#
#     def test_list_blobs(self):
#         async def f():
#             blobs_list = [str(uuid.uuid4()) for i in range(42)]
#             self.azure_blob_api.create_container(self.container_name)
#             for blob_name in blobs_list:
#                 self.azure_blob_api.create_blob_from_bytes(self.container_name, blob_name, uuid.uuid4().bytes)
#             async with aiohttp.ClientSession() as session:
#                 next_marker, async_blobs_list = await self.async_blob_api.list_blobs(self.container_name, session)
#             self.assertEqual(set(blobs_list), set(async_blobs_list))
#         self.async_run(f)
#
#     def test_container_size(self):
#         async def f():
#             blobs_list = [str(uuid.uuid4()) for i in range(42)]
#             self.azure_blob_api.create_container(self.container_name)
#             for blob_name in blobs_list:
#                 self.azure_blob_api.create_blob_from_bytes(self.container_name, blob_name, uuid.uuid4().bytes)
#             async with aiohttp.ClientSession() as session:
#                 size = await self.async_blob_api.container_size(self.container_name, session)
#             self.assertEqual(size, 42*16)
#         self.async_run(f)
#
#     def test_list_blobs_prefix(self):
#         async def f():
#             prefix = '1'
#             blobs_list = [str(uuid.uuid4()) for i in range(42)]
#             self.azure_blob_api.create_container(self.container_name)
#             for blob_name in blobs_list:
#                 self.azure_blob_api.create_blob_from_bytes(self.container_name, blob_name, uuid.uuid4().bytes)
#             async with aiohttp.ClientSession() as session:
#                 next_marker, async_blobs_list = await self.async_blob_api.list_blobs(self.container_name, session,
#                                                                                      prefix=prefix)
#             self.assertEqual(
#                 set([blob_name if blob_name.startswith(prefix) else 'nothing' for blob_name in blobs_list]),
#                 set(async_blobs_list + ['nothing']))
#         self.async_run(f)
#
#     def test_list_blobs_maxresults(self):
#         async def f():
#             maxresults = 42
#             blobs_list = [str(uuid.uuid4()) for i in range(42)]
#             self.azure_blob_api.create_container(self.container_name)
#             for blob_name in blobs_list:
#                 self.azure_blob_api.create_blob_from_bytes(self.container_name, blob_name, uuid.uuid4().bytes)
#             async with aiohttp.ClientSession() as session:
#                 next_marker, async_blobs_list = await self.async_blob_api.list_blobs(self.container_name, session,
#                                                                                      maxresults=maxresults)
#             self.assertEqual(
#                 set(sorted(blobs_list)[:maxresults]), set(async_blobs_list))
#             print(next_marker)
#         self.async_run(f)
#
#     def test_list_blobs_marker(self):
#         async def f():
#             maxresults = 5
#             blobs_list = [str(uuid.uuid4()) for i in range(42)]
#             self.azure_blob_api.create_container(self.container_name)
#             for blob_name in blobs_list:
#                 self.azure_blob_api.create_blob_from_bytes(self.container_name, blob_name, uuid.uuid4().bytes)
#             async with aiohttp.ClientSession() as session:
#                 next_marker, async_blobs_list = await self.async_blob_api.list_blobs(self.container_name, session,
#                                                                                      maxresults=maxresults)
#             self.assertEqual(
#                 set(sorted(blobs_list)[:maxresults]), set(async_blobs_list))
#             # now fetch the next ones using next_marker
#             async with aiohttp.ClientSession() as session:
#                 next_marker, async_blobs_list = await self.async_blob_api.list_blobs(self.container_name, session,
#                                                                                      maxresults=maxresults,
#                                                                                      marker=next_marker)
#             self.assertEqual(
#                 set(sorted(blobs_list)[maxresults:2*maxresults]), set(async_blobs_list))
#         self.async_run(f)
#
#     def test_create_container(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.create_container(self.container_name, session)
#             self.assertTrue(self.azure_blob_api.exists(self.container_name))
#         self.async_run(f)
#
#     def test_acquire_lock(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.create_container(self.container_name, session)
#                 await self.async_blob_api.write_blob_from_text('test', self.container_name, 'test', session)
#                 lease_id = await self.async_blob_api.acquire_lease(self.container_name, 'test', 15, session)
#                 await self.async_blob_api.renew_lease(self.container_name, 'test', lease_id, session)
#                 await self.async_blob_api.release_lease(self.container_name, 'test', lease_id, session)
#         self.async_run(f)
#
#     def test_copy_blob(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.create_container(self.container_name, session)
#                 await self.async_blob_api.write_blob_from_text('test', self.container_name, '~test', session)
#                 status, _ = await self.async_blob_api.copy_blob(self.container_name, '~test', self.container_name, 'test', session)
#                 self.assertTrue(status == 'success')
#         self.async_run(f)
#
#     def test_copy_big_blob(self):
#         async def f():
#             async with aiohttp.ClientSession() as session:
#                 await self.async_blob_api.create_container(self.container_name, session)
#                 big_string = 'test '*10*10**6
#                 await self.async_blob_api.write_blob_from_text(big_string, self.container_name, '~test', session)
#                 status, _ = await self.async_blob_api.copy_blob(self.container_name, '~test', self.container_name, 'test', session)
#                 self.assertTrue(status == 'success')
#         self.async_run(f)