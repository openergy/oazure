# import unittest
#
# from django.db.utils import InterfaceError
# from django import db
# from psycopg2 import OperationalError
#
# from outil.azure.postgresql import DatabaseRetry
#
#
# class PostgresqlTest(unittest.TestCase):
#     i = 0
#
#     def test_simple_retry(self):
#         @DatabaseRetry(1)
#         def f():
#             if self.i == 0:
#                 self.i += 1
#                 raise OperationalError()
#             else:
#                 return 0
#
#         self.assertEqual(f(), 0)
#         self.i = 0
#
#         def f():
#             if self.i == 0:
#                 self.i += 1
#                 raise OperationalError()
#             else:
#                 return 0
#
#         with self.assertRaises(OperationalError):
#             f()
#
#
