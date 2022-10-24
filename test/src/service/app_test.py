from src.service.app import create_app


def test_noop():
    create_app(noop=True)
