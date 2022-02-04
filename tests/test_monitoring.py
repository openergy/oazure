# import aiohttp
#
# from oazure.snippets.testing import AsyncTest
# from oazure.monitoring import LogAnalyticsClient
#
#
# class AzureAsyncBlobOperationsTest(AsyncTest):
#     monitoring_client = LogAnalyticsClient(
#         "",
#         "",
#         "test"
#     )
#     json_data = {
#         "a": 1.2,
#         "b": True,
#         "c": "test"
#     }
#
#     def test_async(self):
#         async def f():
#             self.assertTrue(await self.monitoring_client.send_json_async(self.json_data))
#         self.async_run(f)
#
#     def test_sync(self):
#         self.assertTrue(self.monitoring_client.send_json(self.json_data))
#
