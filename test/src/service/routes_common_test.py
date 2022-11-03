from conftest import assert_exception_correct
from pytest import raises

from src.service.routes_common import err_on_control_chars
from src.service import errors


def test_err_on_control_chars():
    with raises(Exception) as got:
        err_on_control_chars("foo\bbar", "thinger")
    assert_exception_correct(got.value, errors.IllegalParameterError(
        "thinger contains a control character at position 3"))
