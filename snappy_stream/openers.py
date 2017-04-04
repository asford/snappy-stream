import contextlib

from snappy_stream.detail.priv_bytes_owners import priv_openers as _openers

def open_fs_read_bytestream(fpath):
    return _openers.PrivFSBytesOpener.open_read(fpath)

def open_fs_write_bytestream(fpath):
    return _openers.PrivFSBytesOpener.open_write(fpath)


def open_s3_read_bytestream(s3_uri, **kwargs):
    return _openers.PrivS3BytesOpener.open_read(s3_uri, **kwargs)

def open_s3_write_bytestream(s3_uri, **kwargs):
    return _openers.PrivS3BytesOpener.open_write(s3_uri, **kwargs)
