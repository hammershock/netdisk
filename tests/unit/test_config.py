import json
import stat
from pathlib import Path

import pytest

from netdisk.config import (
    Config,
    NetdiskError,
    config_dir_path,
    config_file_path,
    load_config,
    write_config,
)


# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------

class TestConfigPaths:
    def test_config_dir_path_uses_fixed_xdg_location(self, monkeypatch, tmp_path):
        monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
        assert config_dir_path() == tmp_path / '.config' / 'netdisk'

    def test_config_file_path_is_config_json(self, monkeypatch, tmp_path):
        monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
        assert config_file_path() == tmp_path / '.config' / 'netdisk' / 'config.json'


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_nonexistent_file_returns_empty(self, tmp_path):
        assert load_config(tmp_path / 'missing.json') == {}

    def test_loads_simple_dict(self, tmp_path):
        p = tmp_path / 'config.json'
        data = {'token': 'abc123', 'app': 'test'}
        p.write_text(json.dumps(data), encoding='utf-8')
        assert load_config(p) == data

    def test_malformed_json_returns_empty(self, tmp_path):
        p = tmp_path / 'config.json'
        p.write_text('{not valid json', encoding='utf-8')
        assert load_config(p) == {}

    def test_non_dict_payload_returns_empty(self, tmp_path):
        p = tmp_path / 'config.json'
        p.write_text(json.dumps([1, 2, 3]), encoding='utf-8')
        assert load_config(p) == {}


# ---------------------------------------------------------------------------
# write_config
# ---------------------------------------------------------------------------

class TestWriteConfig:
    def test_writes_new_file_and_parent_dir(self, tmp_path):
        p = tmp_path / 'missing-dir' / 'config.json'
        data = {'key': 'value'}
        write_config(data, p)
        assert load_config(p) == data
        assert p.exists()

    def test_overwrites_existing_file(self, tmp_path):
        p = tmp_path / 'config.json'
        write_config({'v': 1}, p)
        write_config({'v': 2}, p)
        assert load_config(p) == {'v': 2}

    def test_file_permissions_are_private(self, tmp_path):
        p = tmp_path / 'config.json'
        write_config({'x': 1}, p)
        assert stat.S_IMODE(p.stat().st_mode) == 0o600

    def test_parent_dir_permissions_are_private(self, tmp_path):
        p = tmp_path / 'config-dir' / 'config.json'
        write_config({'x': 1}, p)
        assert stat.S_IMODE(p.parent.stat().st_mode) == 0o700

    def test_raises_on_non_dict(self, tmp_path):
        with pytest.raises(NetdiskError, match='must be a dict'):
            write_config([], tmp_path / 'config.json')

    def test_no_tmp_file_left_behind(self, tmp_path):
        p = tmp_path / 'config.json'
        write_config({'a': 1}, p)
        leftovers = list(tmp_path.glob('*.tmp'))
        assert leftovers == []


# ---------------------------------------------------------------------------
# Config class
# ---------------------------------------------------------------------------

class TestConfig:
    def _make_config(self, data: dict) -> Config:
        cfg = Config.__new__(Config)
        cfg.data = dict(data)
        return cfg

    def test_init_loads_from_path(self, tmp_path):
        p = tmp_path / 'config.json'
        write_config({'cwd': '/foo'}, p)
        cfg = Config(p)
        assert cfg.data == {'cwd': '/foo'}

    def test_save_writes_to_path(self, tmp_path):
        p = tmp_path / 'config.json'
        cfg = Config(p)
        cfg.data = {'cwd': '/bar'}
        cfg.save()
        assert load_config(p) == {'cwd': '/bar'}

    def test_require_raises_on_missing_keys(self):
        cfg = self._make_config({'client_id': 'x'})
        with pytest.raises(NetdiskError, match='client_secret'):
            cfg.require('client_id', 'client_secret')

    def test_require_passes_when_all_keys_present(self):
        cfg = self._make_config({'a': '1', 'b': '2'})
        cfg.require('a', 'b')  # should not raise

    def test_app_root_property(self):
        cfg = self._make_config({'app_name': 'myapp'})
        assert cfg.app_root == '/apps/myapp'

    def test_app_root_requires_app_name(self):
        cfg = self._make_config({})
        with pytest.raises(NetdiskError):
            _ = cfg.app_root

    def test_cwd_defaults_to_root(self):
        cfg = self._make_config({})
        assert cfg.cwd == '/'

    def test_cwd_returns_stored_value(self):
        cfg = self._make_config({'cwd': '/foo/bar'})
        assert cfg.cwd == '/foo/bar'

    def test_cwd_prepends_slash_if_missing(self):
        cfg = self._make_config({'cwd': 'foo'})
        assert cfg.cwd == '/foo'

    def test_cwd_setter_normalizes_path(self):
        cfg = self._make_config({})
        cfg.cwd = '/a/b/../c'
        assert cfg.data['cwd'] == '/a/c'
