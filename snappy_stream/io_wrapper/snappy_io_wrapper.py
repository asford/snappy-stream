import snappy
from . import WriteWrapper, ReadWrapper
from .chunk_io_wrapper import ChunkWriteWrapper


class SnappyWriteStreamCore(WriteWrapper):
    def __init__(self, sink, owns_sink):
        super().__init__(sink, owns_sink=owns_sink)
        self._compressor = snappy.StreamCompressor()

    def write(self, chunk):
        compressed = self._compressor.add_chunk(chunk)
        self.sink.write(compressed)


class SnappyConsts(object):
    MAX_CHUNK = 65535 # 64kb
    ONE_MB = 1024 * 1024
    TEN_MB = ONE_MB * 10



class SnappyWriteWrapper(WriteWrapper):
    def __init__(self, sink, owns_sink, chunk_size=SnappyConsts.MAX_CHUNK):
        super().__init__(
            ChunkWriteWrapper(
                sink=SnappyWriteStreamCore(sink, owns_sink=owns_sink),
                chunk_size=chunk_size,
                owns_sink=True
            ),
            owns_sink=True
        )



class SnappyReadWrapper(ReadWrapper):
    _decompressor_instance = None
    _buff = b''

    def __init__(self, source, owns_source, chunk_size=SnappyConsts.MAX_CHUNK):
        super().__init__(source, owns_source=owns_source)
        self.chunk_size = chunk_size

    @property
    def _decompressor(self):
        if self._decompressor_instance is None:
            self._decompressor_instance = snappy.StreamDecompressor()
        return self._decompressor_instance

    def _read_chunk(self):
        block = self.source.read(self.chunk_size)
        if not block:
            return b''
        buffed = self._decompressor.decompress(block)
        return buffed

    def read(self, n=-1):
        output = []
        out_len = 0

        chunk = self._buff[:n]
        self._buff = self._buff[n:]
        output.append(chunk)
        out_len = len(chunk)

        while True:
            if n > 0 and out_len >= n:
                break
            if not self._buff:
                next_chunk = self._read_chunk()
                self._buff += next_chunk
                if not self._buff:
                    break
            if n > 0:
                delta = n - out_len
                chunk = self._buff[:delta]
                self._buff = self._buff[delta:]
            else:
                chunk = self._buff
                self._buff = b''
            out_len += len(chunk)
            output.append(chunk)

        return b''.join(output)
