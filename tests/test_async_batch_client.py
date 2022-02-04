# import uuid
# import unittest
#
# from oazure.snippets.testing import AsyncTest
# from oazure.async_batch_client import AzureBatchClient
#
# from .batch_credentials import batch_account_name, batch_account_key, batch_account_url
#
#
# class AzureAsyncBlobOperationsTest(AsyncTest):
#     client = AzureBatchClient(batch_account_name, batch_account_key, batch_account_url)
#     job_id = "test_async_client"
#
#     @classmethod
#     def setUpClass(cls):
#         pass
#
#     @classmethod
#     def tearDownClass(cls):
#         pass
#
#     def setUp(self):
#         pass
#
#     def tearDown(self):
#         pass
#
#     @unittest.SkipTest
#     def test_post_and_get_task(self):
#         async def f():
#             pk = str(uuid.uuid4())
#             await self.client.add_task(
#                 self.job_id,
#                 pk,
#                 command_line="echo 'test'",
#                 environment_settings=[dict(name="test1", value="1"), dict(name="test2", value="2")]
#             )
#             task = await self.client.get_task(self.job_id, pk)
#             self.assertEqual(task["id"], pk)
#             self.assertEqual(
#                 task["environmentSettings"],
#                 [{'name': 'test1', 'value': '1'}, {'name': 'test2', 'value': '2'}]
#             )
#         self.async_run(f)
#
#     def test_clear_job(self):
#         async def f():
#             await self.client.clear_job(self.job_id)
#         self.async_run(f)
