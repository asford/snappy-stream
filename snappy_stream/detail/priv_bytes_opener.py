from contextlib import contextmanager
from snappy_stream.io_wrapper.snappy_io_wrapper import (
    SnappyReadStream,
    SnappyWriteStream
)
from . import HAVE_SMART_OPEN, explode_if_no_smart_open

class PrivBytesOpener(object):
    @classmethod
    def open_under_read(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def open_under_write(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def open_read(cls, *args, **kwargs):
        with cls.open_under_read(*args, **kwargs) as in_buff:
            with SnappyReadStream(in_buff, owns_source=False) as user_buff:
                yield user_buff

    @classmethod
    def open_write(cls, *args, **kwargs):
        with cls.open_under_write(*args, **kwargs) as out_buff:
            with SnappyWriteStream(out_buff, owns_sink=False) as user_buff:
                yield user_buff



class PrivFSBytesOpener(PrivBytesOpener):
    @contextmanager
    @classmethod
    def _fs_open(cls, file_path, mode):
        with open(file_path, mode) as fin:
            yield fin

    @classmethod
    def open_under_read(cls, file_path):
        return cls._fs_open(file_path, 'rb')

    @classmethod
    def open_under_write(cls, file_path):
        return cls._fs_open(file_path, 'wb')


class PrivS3BytesOpener(PrivBytesOpener):
    @contextmanager
    @classmethod
    def _smart_open(cls, s3_uri, mode, **smart_open_kwargs):
        explode_if_no_smart_open()
        import smart_open
        with smart_open.smart_open(s3_uri, mode, **smart_open_kwargs) as fin:
            yield fin

    @classmethod
    def open_under_read(cls, s3_uri, **smart_open_kwargs):
        return cls._smart_open(s3_uri, 'rb', **smart_open_kwargs)

    @classmethod
    def open_under_write(cls, s3_uri, **smart_open_kwargs):
        return cls._smart_open(s3_uri, 'wb', **smart_open_kwargs)
