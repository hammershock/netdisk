"""
Integration tests — hit the real Baidu Netdisk API.

Run with:
    pytest -m integration

All tests operate inside an isolated session-scoped directory
(/_pytest_<random>/) that is cleaned up on teardown.
"""
import os
import time

import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Connectivity / info
# ---------------------------------------------------------------------------

def test_ping(api_client):
    timings = api_client.ping(count=3)
    assert len(timings) == 3
    assert all(t > 0 for t in timings)


def test_quota(api_client):
    q = api_client.quota()
    assert q['quota'] > 0
    assert q['used'] >= 0
    assert q['free'] >= 0
    assert q['used'] + q['free'] == q['quota']


# ---------------------------------------------------------------------------
# Directory operations
# ---------------------------------------------------------------------------

def test_list_dir_returns_list(api_client, test_root):
    items = api_client.list_dir(test_root)
    assert isinstance(items, list)


def test_mkdir_creates_directory(api_client, remote_dir):
    items = api_client.list_dir(remote_dir)
    assert items == []


def test_nested_mkdir(api_client, remote_dir):
    nested = f'{remote_dir}/a/b/c'
    api_client.ensure_remote_dir(nested)
    m = api_client.meta(f'{remote_dir}/a')
    assert m is not None
    assert int(m.get('isdir', 0)) == 1


def test_meta_directory(api_client, remote_dir):
    m = api_client.meta(remote_dir)
    assert m is not None
    assert int(m.get('isdir', 0)) == 1


def test_meta_missing_returns_none(api_client, test_root):
    assert api_client.meta(f'{test_root}/definitely_does_not_exist') is None


# ---------------------------------------------------------------------------
# File upload / download
# ---------------------------------------------------------------------------

def test_upload_and_list(api_client, remote_dir, tmp_path):
    local = tmp_path / 'hello.txt'
    local.write_text('hello netdisk', encoding='utf-8')
    api_client.upload(str(local), f'{remote_dir}/hello.txt')

    names = [i['server_filename'] for i in api_client.list_dir(remote_dir)]
    assert 'hello.txt' in names


def test_upload_and_download_roundtrip(api_client, remote_dir, tmp_path):
    content = b'round-trip test content ' + os.urandom(64)
    local_src = tmp_path / 'src.bin'
    local_src.write_bytes(content)

    remote = f'{remote_dir}/src.bin'
    api_client.upload(str(local_src), remote)

    local_dst = tmp_path / 'dst.bin'
    api_client.download(remote, str(local_dst))
    assert local_dst.read_bytes() == content


def test_upload_large_file_multipart(api_client, remote_dir, tmp_path):
    # 9 MB — forces at least 2 parts (PART_SIZE = 4 MB)
    data = os.urandom(9 * 1024 * 1024)
    local = tmp_path / 'big.bin'
    local.write_bytes(data)

    remote = f'{remote_dir}/big.bin'
    api_client.upload(str(local), remote)

    m = api_client.meta(remote)
    assert m is not None
    assert int(m.get('size', 0)) == len(data)


def test_upload_overwrites_existing(api_client, remote_dir, tmp_path):
    remote = f'{remote_dir}/over.txt'
    (tmp_path / 'v1.txt').write_text('version 1')
    api_client.upload(str(tmp_path / 'v1.txt'), remote)

    (tmp_path / 'v2.txt').write_text('version 2')
    api_client.upload(str(tmp_path / 'v2.txt'), remote)

    dl = tmp_path / 'dl.txt'
    api_client.download(remote, str(dl))
    assert dl.read_text() == 'version 2'


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def test_delete_file(api_client, remote_dir, tmp_path):
    local = tmp_path / 'to_delete.txt'
    local.write_text('bye')
    remote = f'{remote_dir}/to_delete.txt'
    api_client.upload(str(local), remote)

    api_client.delete(remote)
    assert api_client.meta(remote) is None


def test_delete_refuses_root(api_client):
    from netdisk.config import NetdiskError
    with pytest.raises(NetdiskError, match='Refusing to delete'):
        api_client.delete('/')


# ---------------------------------------------------------------------------
# Move / rename
# ---------------------------------------------------------------------------

def test_rename_file(api_client, remote_dir, tmp_path):
    local = tmp_path / 'orig.txt'
    local.write_text('data')
    api_client.upload(str(local), f'{remote_dir}/orig.txt')

    api_client.move(f'{remote_dir}/orig.txt', f'{remote_dir}/renamed.txt')

    assert api_client.meta(f'{remote_dir}/orig.txt') is None
    assert api_client.meta(f'{remote_dir}/renamed.txt') is not None


def test_move_into_subdir(api_client, remote_dir, tmp_path):
    api_client.mkdir(f'{remote_dir}/sub')
    local = tmp_path / 'f.txt'
    local.write_text('move me')
    api_client.upload(str(local), f'{remote_dir}/f.txt')

    api_client.move(f'{remote_dir}/f.txt', f'{remote_dir}/sub/f.txt')
    assert api_client.meta(f'{remote_dir}/sub/f.txt') is not None


# ---------------------------------------------------------------------------
# Tree (recursive) operations
# ---------------------------------------------------------------------------

def test_upload_tree_and_download_tree(api_client, remote_dir, tmp_path):
    # Build local directory tree
    src = tmp_path / 'tree'
    (src / 'sub').mkdir(parents=True)
    (src / 'root.txt').write_text('root')
    (src / 'sub' / 'leaf.txt').write_text('leaf')

    remote_tree = f'{remote_dir}/tree'
    api_client.upload_tree(str(src), remote_tree)

    # Verify structure
    assert api_client.meta(f'{remote_tree}/root.txt') is not None
    assert api_client.meta(f'{remote_tree}/sub/leaf.txt') is not None

    # Download and verify content
    dst = tmp_path / 'dl_tree'
    api_client.download_tree(remote_tree, str(dst))
    assert (dst / 'root.txt').read_text() == 'root'
    assert (dst / 'sub' / 'leaf.txt').read_text() == 'leaf'
