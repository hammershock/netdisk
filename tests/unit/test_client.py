"""Unit tests for BaiduNetdiskClient — all HTTP calls are mocked."""
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from netdisk.client import BaiduNetdiskClient
from netdisk.config import Config, NetdiskError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cfg():
    c = Config.__new__(Config)
    c.data = {
        'client_id': 'test_id',
        'client_secret': 'test_secret',
        'refresh_token': 'test_refresh',
        'access_token': 'cached_token',
        'expires_at': time.time() + 3600,  # valid for 1 more hour
        'app_name': 'test-app',
        'cwd': '/',
    }
    # No-op save so tests never touch the filesystem
    c.save = MagicMock()
    return c


@pytest.fixture
def client(cfg):
    c = BaiduNetdiskClient(cfg)
    c.session = MagicMock()
    return c


def _ok_response(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = body
    resp.status_code = 200
    return resp


def _error_response(errno: int, msg: str = 'bad') -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {'errno': errno, 'errmsg': msg}
    resp.status_code = 200
    return resp


# ---------------------------------------------------------------------------
# ensure_token
# ---------------------------------------------------------------------------

class TestEnsureToken:
    def test_returns_cached_token_when_fresh(self, client):
        token = client.ensure_token()
        assert token == 'cached_token'
        client.session.get.assert_not_called()

    def test_refreshes_when_token_expired(self, client, cfg):
        cfg.data['expires_at'] = time.time() - 1  # already expired
        cfg.data['access_token'] = 'old_token'
        client.session.get.return_value = _ok_response({
            'access_token': 'new_token',
            'refresh_token': 'new_refresh',
            'expires_in': 86400,
        })
        token = client.ensure_token()
        assert token == 'new_token'
        assert cfg.data['access_token'] == 'new_token'
        cfg.save.assert_called_once()

    def test_raises_on_failed_refresh(self, client, cfg):
        cfg.data['expires_at'] = time.time() - 1
        client.session.get.return_value = _ok_response({'error': 'invalid_grant'})
        with pytest.raises(NetdiskError, match='Failed to refresh token'):
            client.ensure_token()


# ---------------------------------------------------------------------------
# _json
# ---------------------------------------------------------------------------

class TestJson:
    def test_returns_data_on_errno_zero(self, client):
        resp = _ok_response({'errno': 0, 'list': []})
        assert client._json(resp) == {'errno': 0, 'list': []}

    def test_returns_data_when_no_errno(self, client):
        resp = _ok_response({'quota': 100})
        assert client._json(resp)['quota'] == 100

    def test_raises_on_nonzero_errno(self, client):
        with pytest.raises(NetdiskError, match='API error'):
            client._json(_error_response(31024))

    def test_errno_attached_to_exception(self, client):
        try:
            client._json(_error_response(31024))
        except NetdiskError as e:
            assert e.errno == 31024

    def test_raises_on_non_json_response(self, client):
        resp = MagicMock()
        resp.json.side_effect = ValueError('no JSON')
        resp.status_code = 200
        resp.text = 'not json'
        with pytest.raises(NetdiskError, match='Non-JSON'):
            client._json(resp)


# ---------------------------------------------------------------------------
# resolve_app_path
# ---------------------------------------------------------------------------

class TestResolveAppPath:
    def test_dot_returns_cwd(self, client, cfg):
        cfg.data['cwd'] = '/mydir'
        assert client.resolve_app_path('.') == '/mydir'

    def test_empty_returns_cwd(self, client, cfg):
        cfg.data['cwd'] = '/mydir'
        assert client.resolve_app_path('') == '/mydir'

    def test_absolute_path_normalized(self, client):
        assert client.resolve_app_path('/foo/bar') == '/foo/bar'

    def test_relative_path_joined_to_cwd(self, client, cfg):
        cfg.data['cwd'] = '/parent'
        assert client.resolve_app_path('child') == '/parent/child'

    def test_dotdot_in_relative_path(self, client, cfg):
        cfg.data['cwd'] = '/a/b'
        assert client.resolve_app_path('../c') == '/a/c'


# ---------------------------------------------------------------------------
# list_dir
# ---------------------------------------------------------------------------

class TestListDir:
    def test_returns_list_from_api(self, client):
        items = [{'server_filename': 'file.txt', 'isdir': 0}]
        client.session.get.return_value = _ok_response({'errno': 0, 'list': items})
        result = client.list_dir('/')
        assert result == items

    def test_empty_list_when_key_absent(self, client):
        client.session.get.return_value = _ok_response({'errno': 0})
        assert client.list_dir('/') == []

    def test_sends_correct_dir_param(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        client.list_dir('/target')
        call_kwargs = client.session.get.call_args
        params = call_kwargs[1]['params']
        assert params['dir'] == '/target'
        assert params['method'] == 'list'

    def test_sends_enhanced_options(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        client.list_dir(
            '/target',
            limit=10,
            order='time',
            desc=True,
            start=5,
            dirs_only=True,
            showempty=True,
            web=False,
        )
        params = client.session.get.call_args[1]['params']
        assert params['order'] == 'time'
        assert params['desc'] == 1
        assert params['start'] == 5
        assert params['limit'] == 10
        assert params['folder'] == 1
        assert params['showempty'] == 1
        assert params['web'] == 0


# ---------------------------------------------------------------------------
# list_all / search / category
# ---------------------------------------------------------------------------

class TestDiscoveryApis:
    def test_list_all_sends_recursive_params(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        client.list_all('/target', recursive=True, order='size', desc=True, start=2, limit=50)
        params = client.session.get.call_args[1]['params']
        assert params['method'] == 'listall'
        assert params['path'] == '/target'
        assert params['recursion'] == 1
        assert params['order'] == 'size'
        assert params['desc'] == 1
        assert params['start'] == 2
        assert params['limit'] == 50

    def test_search_sends_keyword_path_recursion_and_category(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        client.search('report', path='/docs', recursive=True, category='doc', page=2, num=20)
        params = client.session.get.call_args[1]['params']
        assert params['method'] == 'search'
        assert params['key'] == 'report'
        assert params['dir'] == '/docs'
        assert params['recursion'] == 1
        assert params['category'] == 4
        assert params['page'] == 2
        assert params['num'] == 20

    def test_search_rejects_empty_keyword(self, client):
        with pytest.raises(NetdiskError, match='keyword'):
            client.search('')

    def test_category_list_sends_type_and_parent_path(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        client.category_list('image', path='/photos', recursive=True, page=3, num=30)
        params = client.session.get.call_args[1]['params']
        assert params['method'] == 'categorylist'
        assert params['category'] == 3
        assert params['parent_path'] == '/photos'
        assert params['recursion'] == 1
        assert params['page'] == 3
        assert params['num'] == 30

    def test_unknown_category_raises(self, client):
        with pytest.raises(NetdiskError, match='Unknown file type'):
            client.category_list('nonsense')


# ---------------------------------------------------------------------------
# meta
# ---------------------------------------------------------------------------

class TestMeta:
    def test_root_returns_synthetic_entry(self, client):
        m = client.meta('/')
        assert m is not None
        assert int(m['isdir']) == 1

    def test_found_returns_item(self, client):
        items = [
            {'server_filename': 'target.txt', 'isdir': 0, 'size': 42},
            {'server_filename': 'other.txt', 'isdir': 0},
        ]
        client.session.get.return_value = _ok_response({'errno': 0, 'list': items})
        m = client.meta('/some_dir/target.txt')
        assert m is not None
        assert m['server_filename'] == 'target.txt'

    def test_not_found_returns_none(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        assert client.meta('/missing') is None

    def test_missing_parent_returns_none(self, client):
        client.session.get.return_value = _error_response(-9, 'dir not found')
        assert client.meta('/missing/child') is None

    def test_non_missing_api_error_raises(self, client):
        client.session.get.return_value = _error_response(-6, 'permission denied')
        with pytest.raises(NetdiskError):
            client.meta('/some_dir/target.txt')

    def test_file_meta_merges_filemetas_response(self, client):
        file_item = {'server_filename': 'target.txt', 'isdir': 0, 'size': 42, 'fs_id': 123}

        def fake_get(url, params=None, **kwargs):
            params = params or {}
            if params.get('method') == 'filemetas':
                return _ok_response({'errno': 0, 'list': [{'fs_id': 123, 'dlink': 'http://fake'}]})
            return _ok_response({'errno': 0, 'list': [file_item]})

        client.session.get.side_effect = fake_get
        result = client.file_meta('/some_dir/target.txt', dlink=True, thumb=True)
        assert result['server_filename'] == 'target.txt'
        assert result['dlink'] == 'http://fake'
        filemetas_params = client.session.get.call_args_list[-1][1]['params']
        assert filemetas_params['method'] == 'filemetas'
        assert filemetas_params['fsids'] == '[123]'
        assert filemetas_params['dlink'] == 1
        assert filemetas_params['thumb'] == 1

    def test_file_meta_raises_if_missing(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        with pytest.raises(NetdiskError, match='does not exist'):
            client.file_meta('/missing.txt')


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------

class TestMkdir:
    def test_posts_create_request(self, client):
        client.session.post.return_value = _ok_response({'errno': 0})
        client.mkdir('/newdir')
        call_kwargs = client.session.post.call_args
        data = call_kwargs[1]['data']
        assert data['isdir'] == '1'
        assert data['path'] == '/newdir'

    def test_raises_on_api_error(self, client):
        client.session.post.return_value = _error_response(-8, 'path conflict')
        with pytest.raises(NetdiskError):
            client.mkdir('/exists')


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_refuses_to_delete_root(self, client):
        with pytest.raises(NetdiskError, match='Refusing to delete'):
            client.delete('/')

    def test_sends_delete_request(self, client):
        client.session.post.return_value = _ok_response({'errno': 0})
        client.delete('/somefile.txt')
        call_kwargs = client.session.post.call_args
        params = call_kwargs[1]['params']
        assert params['opera'] == 'delete'


# ---------------------------------------------------------------------------
# copy
# ---------------------------------------------------------------------------

class TestCopy:
    def _setup_meta(self, client, items_by_parent: dict):
        def fake_get(url, params=None, **kwargs):
            dir_path = (params or {}).get('dir', '')
            items = items_by_parent.get(dir_path, [])
            return _ok_response({'errno': 0, 'list': items})
        client.session.get.side_effect = fake_get

    def test_refuses_to_copy_root(self, client):
        with pytest.raises(NetdiskError, match='Refusing to copy'):
            client.copy('/', '/dst')

    def test_raises_if_src_missing(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        with pytest.raises(NetdiskError, match='does not exist'):
            client.copy('/missing', '/dst')

    def test_refuses_copy_onto_self(self, client):
        self._setup_meta(client, {
            '/': [{'server_filename': 'f', 'isdir': 0}],
        })
        with pytest.raises(NetdiskError, match='onto itself'):
            client.copy('/f', '/f')

    def test_sends_copy_filemanager_request(self, client):
        self._setup_meta(client, {
            '/': [{'server_filename': 'a.txt', 'isdir': 0}],
        })
        client.session.post.return_value = _ok_response({'errno': 0})
        client.copy('/a.txt', '/b.txt')
        call_kwargs = client.session.post.call_args
        assert call_kwargs[1]['params']['opera'] == 'copy'
        payload = json.loads(call_kwargs[1]['data']['filelist'])
        assert payload == [{
            'path': '/a.txt',
            'dest': '/',
            'newname': 'b.txt',
            'ondup': 'overwrite',
        }]


# ---------------------------------------------------------------------------
# move
# ---------------------------------------------------------------------------

class TestMove:
    def _setup_meta(self, client, items_by_parent: dict):
        """items_by_parent maps parent_path -> list of file items."""
        def fake_get(url, params=None, **kwargs):
            dir_path = (params or {}).get('dir', '')
            items = items_by_parent.get(dir_path, [])
            return _ok_response({'errno': 0, 'list': items})
        client.session.get.side_effect = fake_get

    def test_refuses_to_move_root(self, client):
        with pytest.raises(NetdiskError, match='Refusing to move'):
            client.move('/', '/dst')

    def test_raises_if_src_missing(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        with pytest.raises(NetdiskError, match='does not exist'):
            client.move('/missing', '/dst')

    def test_no_op_when_src_equals_dst(self, client):
        self._setup_meta(client, {
            '/': [{'server_filename': 'f', 'isdir': 0}],
        })
        client.move('/f', '/f')
        client.session.post.assert_not_called()

    def test_raises_if_moving_into_self(self, client):
        self._setup_meta(client, {
            '/': [{'server_filename': 'dir', 'isdir': 1}],
            '/dir': [],
        })
        with pytest.raises(NetdiskError, match='into itself'):
            client.move('/dir', '/dir/sub')

    def test_same_parent_uses_rename(self, client):
        self._setup_meta(client, {
            '/': [{'server_filename': 'a.txt', 'isdir': 0}],
        })
        client.session.post.return_value = _ok_response({'errno': 0})
        client.move('/a.txt', '/b.txt')
        call_kwargs = client.session.post.call_args
        assert call_kwargs[1]['params']['opera'] == 'rename'


# ---------------------------------------------------------------------------
# tree
# ---------------------------------------------------------------------------

class TestListTree:
    def test_returns_depth_item_pairs(self, client):
        def fake_get(url, params=None, **kwargs):
            dir_path = (params or {}).get('dir', '')
            items_by_parent = {
                '/': [
                    {'server_filename': 'dir', 'path': '/dir', 'isdir': 1},
                    {'server_filename': 'f.txt', 'path': '/f.txt', 'isdir': 0},
                ],
                '/dir': [
                    {'server_filename': 'nested.txt', 'path': '/dir/nested.txt', 'isdir': 0},
                ],
            }
            return _ok_response({'errno': 0, 'list': items_by_parent.get(dir_path, [])})

        client.session.get.side_effect = fake_get
        entries = client.list_tree('/', max_depth=2)
        assert [depth for depth, _ in entries] == [0, 1, 2, 1]
        assert [item['server_filename'] for _, item in entries[1:]] == ['dir', 'nested.txt', 'f.txt']

    def test_respects_max_depth(self, client):
        def fake_get(url, params=None, **kwargs):
            dir_path = (params or {}).get('dir', '')
            items_by_parent = {
                '/': [{'server_filename': 'dir', 'path': '/dir', 'isdir': 1}],
                '/dir': [{'server_filename': 'nested.txt', 'path': '/dir/nested.txt', 'isdir': 0}],
            }
            return _ok_response({'errno': 0, 'list': items_by_parent.get(dir_path, [])})

        client.session.get.side_effect = fake_get
        entries = client.list_tree('/', max_depth=1)
        assert [item['server_filename'] for _, item in entries[1:]] == ['dir']


# ---------------------------------------------------------------------------
# upload (unit — no real file I/O beyond a tiny tmp file)
# ---------------------------------------------------------------------------

class TestUpload:
    def test_raises_if_local_missing(self, client, tmp_path):
        with pytest.raises(NetdiskError, match='does not exist'):
            client.upload(str(tmp_path / 'ghost.txt'), '/remote.txt')

    def test_raises_if_local_is_directory(self, client, tmp_path):
        d = tmp_path / 'mydir'
        d.mkdir()
        with pytest.raises(NetdiskError, match='upload_tree'):
            client.upload(str(d), '/remote')

    def test_uploads_file_successfully(self, client, tmp_path):
        local = tmp_path / 'hello.txt'
        local.write_bytes(b'hello')

        # meta(remote) → not a directory
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        # precreate → uploadid
        # part upload → ok
        # create (commit) → ok
        responses = [
            _ok_response({'errno': 0, 'uploadid': 'uid1', 'path': '/remote.txt'}),
            _ok_response({'errno': 0}),  # part upload
            _ok_response({'errno': 0}),  # commit
        ]
        client.session.post.side_effect = responses
        client.upload(str(local), '/remote.txt')
        assert client.session.post.call_count == 3


# ---------------------------------------------------------------------------
# download (unit)
# ---------------------------------------------------------------------------

class TestDownload:
    def test_raises_if_remote_missing(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        with pytest.raises(NetdiskError, match='does not exist'):
            client.download('/missing.txt', '/tmp/out.txt')

    def test_raises_if_remote_is_dir(self, client):
        client.session.get.return_value = _ok_response({
            'errno': 0,
            'list': [{'server_filename': 'mydir', 'isdir': 1}],
        })
        with pytest.raises(NetdiskError, match='download_tree'):
            client.download('/mydir', '/tmp/out')

    def test_downloads_file(self, client, tmp_path):
        file_meta = {'server_filename': 'f.txt', 'isdir': 0, 'size': 5, 'fs_id': 999}
        dlink_resp = _ok_response({'errno': 0, 'list': [{'dlink': 'http://fake'}]})

        def fake_get(url, params=None, headers=None, stream=False, **kwargs):
            if stream:
                # Simulate file download
                r = MagicMock()
                r.status_code = 200
                r.iter_content.return_value = [b'hello']
                return r
            params = params or {}
            if params.get('method') == 'filemetas':
                return dlink_resp
            # list_dir call
            return _ok_response({'errno': 0, 'list': [file_meta]})

        client.session.get.side_effect = fake_get
        out = tmp_path / 'out.txt'
        client.download('/dir/f.txt', str(out))
        assert out.read_bytes() == b'hello'


# ---------------------------------------------------------------------------
# quota / ping
# ---------------------------------------------------------------------------

class TestQuota:
    def test_returns_usage_dict(self, client):
        client.session.get.return_value = _ok_response({
            'errno': 0, 'quota': 2048, 'used': 512,
        })
        q = client.quota()
        assert q == {'quota': 2048, 'used': 512, 'free': 1536}

    def test_free_is_non_negative(self, client):
        client.session.get.return_value = _ok_response({
            'errno': 0, 'quota': 100, 'used': 200,
        })
        q = client.quota()
        assert q['free'] == 0


class TestPing:
    def test_returns_list_of_timings(self, client):
        client.session.get.return_value = _ok_response({'errno': 0, 'list': []})
        timings = client.ping(count=2)
        assert len(timings) == 2
        assert all(t >= 0 for t in timings)
