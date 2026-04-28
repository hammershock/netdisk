"""
Integration test setup.

Credentials are loaded in priority order:
  1. tests/credentials.json  (gitignored — create this manually)
  2. ~/.config/netdisk/config.json  (populated by `netdisk login`)

credentials.json format:
    {
        "client_id": "...",
        "client_secret": "...",
        "app_name": "netdisk-cli",
        "refresh_token": "..."
    }

Run integration tests:
    pytest -m integration
Skip integration tests (default):
    pytest -m "not integration"
"""
import json
import uuid
from pathlib import Path

import pytest

from netdisk.client import BaiduNetdiskClient
from netdisk.config import Config, load_config


def _load_credentials() -> dict:
    creds_file = Path(__file__).parent / 'credentials.json'
    if creds_file.exists():
        data = json.loads(creds_file.read_text())
        if data.get('refresh_token'):
            return data

    data = load_config()
    if data.get('refresh_token'):
        return data

    return {}


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'integration: requires real Baidu Netdisk credentials (skipped by default)',
    )


@pytest.fixture(scope='session')
def api_client():
    data = _load_credentials()
    if not data.get('refresh_token'):
        pytest.skip(
            'No credentials found.\n'
            '  Option 1: run `netdisk login` to save credentials\n'
            '  Option 2: create tests/credentials.json with client_id, client_secret, '
            'app_name, refresh_token'
        )
    cfg = Config.__new__(Config)
    cfg.data = dict(data)
    cfg.data.setdefault('cwd', '/')
    return BaiduNetdiskClient(cfg)


@pytest.fixture(scope='session')
def test_root(api_client) -> str:
    """A unique remote directory created for this test session, cleaned up afterward."""
    path = f'/_pytest_{uuid.uuid4().hex[:8]}'
    api_client.mkdir(path)
    yield path
    try:
        # Remove all contents then the directory itself
        def _rm_all(p: str):
            for item in api_client.list_dir(p):
                child = f"{p}/{item['server_filename']}"
                if int(item.get('isdir', 0)):
                    _rm_all(child)
                else:
                    api_client.delete(child)
            api_client.delete(p)
        _rm_all(path)
    except Exception:
        pass


@pytest.fixture
def remote_dir(api_client, test_root) -> str:
    """A fresh sub-directory inside test_root, deleted after each test."""
    name = uuid.uuid4().hex[:8]
    path = f'{test_root}/{name}'
    api_client.mkdir(path)
    yield path
    try:
        def _rm_all(p: str):
            for item in api_client.list_dir(p):
                child = f"{p}/{item['server_filename']}"
                if int(item.get('isdir', 0)):
                    _rm_all(child)
                else:
                    api_client.delete(child)
            api_client.delete(p)
        _rm_all(path)
    except Exception:
        pass
