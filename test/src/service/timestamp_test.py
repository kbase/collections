from src.service.timestamp import timestamp
from conftest import assert_close_to_now

def test_timestamp():
    ts = timestamp()
    assert_close_to_now(ts)
