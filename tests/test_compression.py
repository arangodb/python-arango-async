from arangoasync.compression import AcceptEncoding, DefaultCompressionManager


def test_DefaultCompressionManager_no_compression():
    manager = DefaultCompressionManager()
    assert not manager.needs_compression("test")
    assert not manager.needs_compression(b"test")
    manager = DefaultCompressionManager(threshold=10)
    assert not manager.needs_compression("test")


def test_DefaultCompressionManager_compress():
    manager = DefaultCompressionManager(
        threshold=1, level=9, accept=AcceptEncoding.DEFLATE
    )
    data = "a" * 10 + "b" * 10
    assert manager.needs_compression(data)
    assert len(manager.compress(data)) < len(data)
    assert manager.content_encoding == "deflate"
    assert manager.accept_encoding == "deflate"
    data = b"a" * 10 + b"b" * 10
    assert manager.needs_compression(data)
    assert len(manager.compress(data)) < len(data)


def test_DefaultCompressionManager_properties():
    manager = DefaultCompressionManager(
        threshold=1, level=9, accept=AcceptEncoding.DEFLATE
    )
    assert manager.threshold == 1
    assert manager.level == 9
    assert manager.accept_encoding == "deflate"
    assert manager.content_encoding == "deflate"
    manager.threshold = 10
    assert manager.threshold == 10
    manager.level = 2
    assert manager.level == 2
    manager.accept_encoding = AcceptEncoding.GZIP
    assert manager.accept_encoding == "gzip"
