from src.service.config import CollectionsServiceConfig
from io import BytesIO


# TODO TEST more tests


def test_config_minimal():
    cfg = CollectionsServiceConfig(BytesIO("\n".join([
        "[Arango]",
        'url="foo"',
        'database="bar"',
        "[Authentication]",
        'url="foobar"',
        "[Service]",
        "[Service_Dependencies]",
        'workspace_url="whee"'
        ]).encode('utf-8')
    ))
    assert cfg.arango_url == "foo"
    assert cfg.arango_db == "bar"
    assert cfg.arango_user == None
    assert cfg.arango_pwd == None
    assert cfg.auth_url == "foobar"
    assert cfg.auth_full_admin_roles == []
    assert cfg.service_root_path == None
    assert cfg.workspace_url == "whee"
