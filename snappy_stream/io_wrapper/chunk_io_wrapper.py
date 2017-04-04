from . import WriteWrapper

class ChunkWriteWrapper(WriteWrapper):
    def __init__(self, sink, chunk_size, owns_sink):
        super(ChunkWriteWrapper, self).__init__(sink, owns_sink)
        self.chunk_size = chunk_size
        self._chunks = []
        self._chunks_len = 0

    def _drain(self):
        if not self._chunks:
            return
        remaining = b''.join(self._chunks)
        self._chunks = []
        self._chunks_len = 0
        while remaining:
            current = remaining[:self.chunk_size]
            remaining = remaining[self.chunk_size:]
            self._write_chunk(current)


    def close(self):
        super(ChunkWriteWrapper, self).close()

    def flush_self(self):
        self._drain()

    def _write_chunk(self, chunk):
        assert len(chunk) <= self.chunk_size
        self.sink.write(chunk)

    def write(self, chunk):
        self._chunks.append(chunk)
        self._chunks_len += len(chunk)
        lim = self.chunk_size
        if self._chunks_len >= lim:
            full_chunk = b''.join(self._chunks)
            to_write = full_chunk[:lim]
            self._chunks = [full_chunk[lim:]]
            self._chunks_len = len(self._chunks[0])
            self._write_chunk(to_write)

