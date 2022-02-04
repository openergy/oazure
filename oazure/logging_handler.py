import logging
import socket
try:
    from django.core.handlers.wsgi import WSGIRequest
except ImportError:
    WSGIRequest = None

from applicationinsights.logging import LoggingHandler

RESERVED_ATTRS = (
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName"
)


class AzureLoggingHandler(LoggingHandler):
    def __init__(self, instrumentation_key, component_name, *args, **kwargs):
        self.hostname = socket.gethostname()
        self.component_name = component_name
        super().__init__(instrumentation_key, *args, **kwargs)

    def emit(self, record):
        """Emit a record.

        If a formatter is specified, it is used to format the record. If exception information is present, an Exception
        telemetry object is sent instead of a Trace telemetry object.

        Args:
            record (:class:`logging.LogRecord`). the record to format and send.
        """
        # the set of properties that will ride with the record
        properties = dict(
            process=record.processName,
            module=record.name,
            filename=record.filename,
            line_number=record.lineno,
            level=record.levelname,
            hostname=self.hostname,
            component=self.component_name,
            **dict((f'_{key}', record.__dict__[key]) for key in record.__dict__ if key not in RESERVED_ATTRS)
        )

        # Bad hack for django, find a better way...
        if WSGIRequest is not None and isinstance(properties.get("_request"), WSGIRequest):
            properties["_request"] = str(properties["_request"])

        # if we have exec_info, we will use it as an exception
        if record.exc_info:
            self.client.track_exception(*record.exc_info, properties=properties)
        else:
            # if we don't simply format the message and send the trace
            formatted_message = self.format(record)
            self.client.track_trace(formatted_message, properties=properties, severity=record.levelname)
        # We flush immediately for anything above warnings (for info, etc. will be flushed when the queue has 500 msgs)
        if record.levelno >= logging.WARNING:
            self.client.flush()
