"""
opysnippets/testing:1.0.0
"""
import logging
import unittest
import asyncio
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
# from nose.tools import nottest


logger = logging.getLogger(__name__)


# @nottest  # prevent from nose call
def abstract_test(method):
    """
    Decorator for methods of abstract test classes
    """
    def test_wrapped(self, *args, **kwargs):
        method_class_qualified_name = ".".join(method.__qualname__.split(".")[:-1])
        if self.__class__.__qualname__ == method_class_qualified_name:  # abstract
            return None
        # for nose
        if self.__class__.__qualname__ == 'transplant_class.<locals>.C':
            return None
        return method(self, *args, **kwargs)
    return test_wrapped


class AsyncTest(unittest.TestCase):
    """
    Compatible with unittests and nose.
    """
    async_default_shutdown_timeout = 5

    @classmethod
    def async_run(cls, f, *args, **kwargs):
        """method name must not contain word test, or else will fail when using nose"""
        coro = asyncio.coroutine(f)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(coro(*args, **kwargs))
        # ensure all tasks have completed
        try:
            ongoing_tasks = [t for t in asyncio.Task.all_tasks() if not t.done()]
            if len(ongoing_tasks) > 0:
                logger.warning(
                    "tasks are still ongoing although run_until_complete ended",
                    extra=dict(ongoing_tasks="\n - ".join([str(t) for t in ongoing_tasks]))
                )

            loop.run_until_complete(
               asyncio.wait_for(asyncio.gather(*ongoing_tasks), cls.async_default_shutdown_timeout))
        except asyncio.CancelledError:
            # In case we cancel some tasks
            pass
        except ConcurrentTimeoutError as e:
            logger.critical("tasks still remain after gather timeout")
            for t in asyncio.Task.all_tasks():
                t.cancel()
            raise e


class ConfTempSetter:
    def __init__(self, CONF, **kwargs):
        self._CONF = CONF
        self._modifications = kwargs

    def __call__(self, fct):
        def test_wrapped(wrapped_self, *args, **kwargs):
            current_conf = {}
            for key in self._modifications:
                current_conf[key] = getattr(self._CONF, key)
                setattr(self._CONF, key, self._modifications[key])
            try:
                return fct(wrapped_self, *args, **kwargs)
            finally:
                for key in self._modifications:
                    setattr(self._CONF, key, current_conf[key])
        return test_wrapped
