from src.common.hash import md5_string


def test_md5_string():
    md = md5_string("foo")
    assert md == "acbd18db4cc2f85cedef654fccc4a4d8"