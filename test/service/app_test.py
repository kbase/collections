# not sure I really like having a global app... but that's how python does it
from service.app import app
from fastapi.testclient import TestClient


def test_read_main():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {
        'git_hash': 'fake_commit',
        'service': 'Collections',
        'version': '0.1.0-prototype1'
    }