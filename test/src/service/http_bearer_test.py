from src.service.http_bearer import KBaseHTTPBearer


def test_noop():
    khp = KBaseHTTPBearer()
    assert khp.scheme_name == "KBaseHTTPBearer"
