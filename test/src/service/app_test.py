from src.service.app import create_app
from fastapi.testclient import TestClient

from conftest import assert_close_to_now


def test_read_main():
    app = create_app()
    client = TestClient(app)
    response = client.get("/")
    res = response.json()
    servertime = res.pop('server_time')
    assert res == {
        'git_hash': 'fake_commit',
        'service_name': 'Collections Prototype',
        'version': '0.1.0-prototype1'
    }
    assert_close_to_now(servertime)
    assert response.status_code == 200