from src.service.models import DataProduct


# TODO TEST add more tests


def test_noop():
    dp = DataProduct(product="foo", version="bar")
    assert dp.product == "foo"
    assert dp.version == "bar"