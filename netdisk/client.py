import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

import requests

from .config import Config, NetdiskError
from .constants import (
    PART_SIZE,
    PCS_QUOTA_URL,
    SUPERFILE_URL,
    TOKEN_URL,
    XPAN_FILE_URL,
    XPAN_MULTIMEDIA_URL,
)
from .utils import (
    ProgressPrinter,
    _is_dir,
    has_trailing_slash,
    make_session,
    normalize_app_path,
)


CATEGORY_TYPES = {
    'video': 1,
    'audio': 2,
    'image': 3,
    'doc': 4,
    'document': 4,
    'app': 5,
    'other': 6,
    'bt': 7,
    'torrent': 7,
}


class BaiduNetdiskClient:
    def __init__(self, cfg: Config, via_proxy: bool = False):
        self.cfg = cfg
        self.session = make_session(via_proxy=via_proxy)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def ensure_token(self) -> str:
        self.cfg.require('client_id', 'client_secret', 'refresh_token')
        expires_at = float(self.cfg.data.get('expires_at', 0))
        access_token = self.cfg.data.get('access_token', '')
        if access_token and time.time() < expires_at - 300:
            return access_token

        resp = self.session.get(
            TOKEN_URL,
            params={
                'grant_type': 'refresh_token',
                'refresh_token': self.cfg.data['refresh_token'],
                'client_id': self.cfg.data['client_id'],
                'client_secret': self.cfg.data['client_secret'],
            },
            timeout=30,
        )
        data = resp.json()
        if 'access_token' not in data:
            raise NetdiskError(f'Failed to refresh token: {data}')
        self.cfg.data['access_token'] = data['access_token']
        self.cfg.data['refresh_token'] = data.get('refresh_token', self.cfg.data['refresh_token'])
        self.cfg.data['expires_at'] = time.time() + int(data.get('expires_in', 86400))
        self.cfg.save()
        return self.cfg.data['access_token']

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict, stream: bool = False) -> requests.Response:
        params = dict(params)
        params['access_token'] = self.ensure_token()
        return self.session.get(url, params=params, timeout=300, stream=stream)

    def _post(self, url: str, params: dict, data=None, files=None) -> requests.Response:
        params = dict(params)
        params['access_token'] = self.ensure_token()
        return self.session.post(url, params=params, data=data, files=files, timeout=300)

    def _json(self, resp: requests.Response) -> dict:
        try:
            data = resp.json()
        except Exception as e:
            raise NetdiskError(
                f'Non-JSON API response: {resp.status_code} {resp.text[:200]}'
            ) from e
        errno = data.get('errno')
        if errno not in (None, 0, '0'):
            err = NetdiskError(f'API error: {data}')
            try:
                err.errno = int(errno)
            except Exception:
                err.errno = errno
            err.payload = data
            raise err
        return data

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def resolve_app_path(self, path: str) -> str:
        if not path or path == '.':
            return self.cfg.cwd
        if path.startswith('/'):
            return normalize_app_path(path)
        return normalize_app_path(self.cfg.cwd.rstrip('/') + '/' + path)

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    def _category_id(self, category) -> int:
        if isinstance(category, int):
            return category
        value = str(category).strip().lower()
        if value.isdigit():
            return int(value)
        if value not in CATEGORY_TYPES:
            raise NetdiskError(f'Unknown file type: {category}')
        return CATEGORY_TYPES[value]

    def list_dir(
        self,
        path: str,
        limit: int = 1000,
        order: str = 'name',
        desc: bool = False,
        start: int = 0,
        dirs_only: bool = False,
        showempty: bool = False,
        web: bool = True,
    ) -> list:
        app_path = self.resolve_app_path(path)
        data = self._json(self._get(XPAN_FILE_URL, {
            'method': 'list',
            'dir': app_path,
            'order': order,
            'desc': 1 if desc else 0,
            'start': start,
            'limit': limit,
            'web': 1 if web else 0,
            'folder': 1 if dirs_only else 0,
            'showempty': 1 if showempty else 0,
        }))
        return data.get('list', [])

    def list_all(
        self,
        path: str = '.',
        recursive: bool = True,
        limit: int = 1000,
        order: str = 'name',
        desc: bool = False,
        start: int = 0,
    ) -> list:
        app_path = self.resolve_app_path(path)
        data = self._json(self._get(XPAN_MULTIMEDIA_URL, {
            'method': 'listall',
            'path': app_path,
            'recursion': 1 if recursive else 0,
            'order': order,
            'desc': 1 if desc else 0,
            'start': start,
            'limit': limit,
            'web': 1,
        }))
        return data.get('list', [])

    def search(
        self,
        key: str,
        path: str = '.',
        recursive: bool = False,
        category=None,
        page: int = 1,
        num: int = 500,
    ) -> list:
        if not key:
            raise NetdiskError('Search keyword cannot be empty')
        params = {
            'method': 'search',
            'key': key,
            'dir': self.resolve_app_path(path),
            'recursion': 1 if recursive else 0,
            'page': page,
            'num': num,
            'web': 1,
        }
        if category is not None:
            params['category'] = self._category_id(category)
        data = self._json(self._get(XPAN_FILE_URL, params))
        return data.get('list', [])

    def category_list(
        self,
        category,
        path: str = '.',
        recursive: bool = False,
        page: int = 1,
        num: int = 500,
        order: str = 'time',
        desc: bool = True,
    ) -> list:
        data = self._json(self._get(XPAN_MULTIMEDIA_URL, {
            'method': 'categorylist',
            'category': self._category_id(category),
            'parent_path': self.resolve_app_path(path),
            'recursion': 1 if recursive else 0,
            'page': page,
            'num': num,
            'order': order,
            'desc': 1 if desc else 0,
            'web': 1,
        }))
        return data.get('list', [])

    def meta(self, path: str) -> Optional[dict]:
        app_path = self.resolve_app_path(path)
        if app_path == '/':
            return {'isdir': 1, 'path': self.cfg.app_root, 'server_filename': self.cfg.data['app_name']}
        parent = normalize_app_path(os.path.dirname(app_path))
        name = os.path.basename(app_path)
        try:
            items = self.list_dir(parent)
        except NetdiskError as e:
            if getattr(e, 'errno', None) == -9:
                return None
            raise
        for item in items:
            if item.get('server_filename') == name:
                return item
        return None

    def mkdir(self, path: str):
        app_path = self.resolve_app_path(path)
        self._json(self._post(XPAN_FILE_URL, {'method': 'create'}, data={
            'path': app_path,
            'isdir': '1',
            'size': '0',
            'block_list': '[]',
        }))

    def ensure_remote_dir(self, path: str):
        app_path = self.resolve_app_path(path)
        meta = self.meta(app_path)
        if meta:
            if not _is_dir(meta):
                raise NetdiskError(f'Remote path exists and is not a directory: {path}')
            return
        # Walk up to find the first existing ancestor, then create downward.
        missing = []
        cur = app_path
        while cur not in ('', '/'):
            if self.meta(cur):
                break
            missing.append(cur)
            cur = normalize_app_path(os.path.dirname(cur))
        for d in reversed(missing):
            self.mkdir(d)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    def file_meta(
        self,
        path: str,
        dlink: bool = False,
        thumb: bool = False,
        extra: bool = False,
        needmedia: bool = False,
        detail: bool = False,
    ) -> dict:
        basic = self.meta(path)
        if not basic:
            raise NetdiskError(f'Remote path does not exist: {path}')
        fsid = basic.get('fs_id') or basic.get('fsid')
        if not fsid:
            return basic
        data = self._json(self._get(XPAN_MULTIMEDIA_URL, {
            'method': 'filemetas',
            'fsids': json.dumps([int(fsid)]),
            'dlink': 1 if dlink else 0,
            'thumb': 1 if thumb else 0,
            'extra': 1 if extra else 0,
            'needmedia': 1 if needmedia else 0,
            'detail': 1 if detail else 0,
        }))
        items = data.get('list', [])
        if not items:
            return basic
        merged = dict(basic)
        merged.update(items[0])
        return merged

    def delete(self, path: str):
        app_path = self.resolve_app_path(path)
        if app_path == '/':
            raise NetdiskError('Refusing to delete app root / for safety')
        self._json(self._post(
            XPAN_FILE_URL,
            {'method': 'filemanager', 'opera': 'delete'},
            data={'async': '0', 'filelist': json.dumps([{'path': app_path}], ensure_ascii=False)},
        ))

    def _api_dst_parent(self, src_path: str, dst_parent: str) -> str:
        if src_path.startswith(self.cfg.app_root.rstrip('/') + '/'):
            return self.cfg.app_root.rstrip('/') + dst_parent
        if src_path == self.cfg.app_root:
            return (
                self.cfg.app_root if dst_parent == '/'
                else self.cfg.app_root.rstrip('/') + dst_parent
            )
        return dst_parent

    def copy(self, src: str, dst: str):
        src_path = self.resolve_app_path(src)
        if src_path == '/':
            raise NetdiskError('Refusing to copy app root / for safety')
        src_meta = self.meta(src_path)
        if not src_meta:
            raise NetdiskError(f'Remote path does not exist: {src}')

        dst_meta = self.meta(dst)
        if dst.endswith('/') or (dst_meta and _is_dir(dst_meta)):
            base = os.path.basename(src_path.rstrip('/'))
            dst_path = normalize_app_path(self.resolve_app_path(dst).rstrip('/') + '/' + base)
        else:
            dst_path = self.resolve_app_path(dst)

        if dst_path == '/':
            raise NetdiskError('Refusing to overwrite app root / for safety')
        if src_path == dst_path:
            raise NetdiskError('Cannot copy a path onto itself')
        if _is_dir(src_meta) and dst_path.startswith(src_path.rstrip('/') + '/'):
            raise NetdiskError('Cannot copy a directory into itself')

        existing_dst = self.meta(dst_path)
        if existing_dst and _is_dir(existing_dst) and not _is_dir(src_meta):
            raise NetdiskError(f'Destination is a directory: {dst}')

        dst_parent = normalize_app_path(os.path.dirname(dst_path))
        new_name = os.path.basename(dst_path)
        self._json(self._post(
            XPAN_FILE_URL,
            {'method': 'filemanager', 'opera': 'copy'},
            data={
                'async': '0',
                'filelist': json.dumps([
                    {
                        'path': src_path,
                        'dest': self._api_dst_parent(src_path, dst_parent),
                        'newname': new_name,
                        'ondup': 'overwrite',
                    }
                ], ensure_ascii=False),
            },
        ))

    def move(self, src: str, dst: str):
        src_path = self.resolve_app_path(src)
        if src_path == '/':
            raise NetdiskError('Refusing to move app root / for safety')
        src_meta = self.meta(src_path)
        if not src_meta:
            raise NetdiskError(f'Remote path does not exist: {src}')

        dst_meta = self.meta(dst)
        if dst.endswith('/') or (dst_meta and _is_dir(dst_meta)):
            base = os.path.basename(src_path.rstrip('/'))
            dst_path = normalize_app_path(self.resolve_app_path(dst).rstrip('/') + '/' + base)
        else:
            dst_path = self.resolve_app_path(dst)

        if dst_path == '/':
            raise NetdiskError('Refusing to overwrite app root / for safety')
        if src_path == dst_path:
            return
        if dst_path.startswith(src_path.rstrip('/') + '/'):
            raise NetdiskError('Cannot move a directory into itself')

        existing_dst = self.meta(dst_path)
        if existing_dst:
            if _is_dir(existing_dst) and not _is_dir(src_meta):
                raise NetdiskError(f'Destination is a directory: {dst}')
            self.delete(dst_path)

        src_parent = normalize_app_path(os.path.dirname(src_path))
        dst_parent = normalize_app_path(os.path.dirname(dst_path))
        new_name = os.path.basename(dst_path)

        if src_parent == dst_parent:
            self._json(self._post(
                XPAN_FILE_URL,
                {'method': 'filemanager', 'opera': 'rename'},
                data={
                    'async': '0',
                    'filelist': json.dumps(
                        [{'path': src_path, 'newname': new_name}], ensure_ascii=False
                    ),
                },
            ))
            return

        self._json(self._post(
            XPAN_FILE_URL,
            {'method': 'filemanager', 'opera': 'move'},
            data={
                'async': '0',
                'filelist': json.dumps([
                    {
                        'path': src_path,
                        'dest': self._api_dst_parent(src_path, dst_parent),
                        'newname': new_name,
                        'ondup': 'overwrite',
                    }
                ], ensure_ascii=False),
            },
        ))

    def get_dlink(self, fsid: int) -> str:
        data = self._json(self._get(XPAN_MULTIMEDIA_URL, {
            'method': 'filemetas',
            'fsids': json.dumps([int(fsid)]),
            'dlink': 1,
        }))
        items = data.get('list', [])
        if not items:
            raise NetdiskError('Failed to get download link')
        return items[0]['dlink']

    def download(self, remote: str, local: str):
        m = self.meta(remote)
        if not m:
            raise NetdiskError(f'Remote file does not exist: {remote}')
        if _is_dir(m):
            raise NetdiskError('Use download_tree for directories')
        fsid = m.get('fs_id') or m.get('fsid')
        if not fsid:
            raise NetdiskError('Missing fs_id, cannot download')
        dlink = self.get_dlink(int(fsid))

        local_path = Path(local).expanduser()
        if local.endswith('/') or local_path.is_dir():
            local_path = local_path / m['server_filename']
        local_path.parent.mkdir(parents=True, exist_ok=True)

        total_size = int(m.get('size', 0)) if m.get('size') is not None else None
        progress = ProgressPrinter('Downloading', total_size)
        resp = self.session.get(
            dlink,
            params={'access_token': self.ensure_token()},
            headers={'User-Agent': 'pan.baidu'},
            stream=True,
            timeout=300,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            raise NetdiskError(f'Download failed: HTTP {resp.status_code}')

        downloaded = 0
        part_path = local_path.parent / f'.{local_path.name}.part-{os.getpid()}'
        try:
            with open(part_path, 'wb') as f:
                for chunk in resp.iter_content(1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    progress.update(downloaded, force=(downloaded == total_size if total_size else False))
            progress.finish()
            os.replace(part_path, local_path)
        except Exception:
            part_path.unlink(missing_ok=True)
            raise
        print(f'OK: downloaded {remote} -> {local_path}')

    def upload(self, local: str, remote: str):
        local_path = Path(local).expanduser()
        if not local_path.exists():
            raise NetdiskError(f'Local file does not exist: {local}')
        if local_path.is_dir():
            raise NetdiskError('Use upload_tree for directories')

        remote_meta = self.meta(remote) if remote else None
        if not remote or remote.endswith('/') or (remote_meta and _is_dir(remote_meta)):
            base = remote if remote else '.'
            remote_path = normalize_app_path(
                (base if base.startswith('/') else self.cfg.cwd.rstrip('/') + '/' + base).rstrip('/')
                + '/' + local_path.name
            )
        elif remote.startswith('/'):
            remote_path = normalize_app_path(remote)
        else:
            remote_path = normalize_app_path(self.cfg.cwd.rstrip('/') + '/' + remote)

        size = local_path.stat().st_size
        block_list = []
        hash_progress = ProgressPrinter('Hashing', size)
        with open(local_path, 'rb') as f:
            hashed = 0
            while True:
                chunk = f.read(PART_SIZE)
                if not chunk:
                    break
                block_list.append(hashlib.md5(chunk).hexdigest())
                hashed += len(chunk)
                hash_progress.update(hashed, force=(hashed == size))
        hash_progress.finish()

        pre = self._json(self._post(XPAN_FILE_URL, {'method': 'precreate'}, data={
            'path': remote_path,
            'size': str(size),
            'isdir': '0',
            'autoinit': '1',
            'rtype': '3',
            'block_list': json.dumps(block_list),
        }))
        uploadid = pre['uploadid']
        actual_remote_path = pre.get('path', remote_path)

        upload_progress = ProgressPrinter('Uploading', size)
        try:
            with open(local_path, 'rb') as f:
                partseq = 0
                uploaded = 0
                while True:
                    chunk = f.read(PART_SIZE)
                    if not chunk:
                        break
                    self._json(self._post(SUPERFILE_URL, {
                        'method': 'upload',
                        'type': 'tmpfile',
                        'path': actual_remote_path,
                        'uploadid': uploadid,
                        'partseq': partseq,
                    }, files={'file': ('blob', chunk)}))
                    partseq += 1
                    uploaded += len(chunk)
                    upload_progress.update(uploaded, force=(uploaded == size))
            upload_progress.finish()

            self._json(self._post(XPAN_FILE_URL, {'method': 'create'}, data={
                'path': remote_path,
                'size': str(size),
                'isdir': '0',
                'rtype': '3',
                'uploadid': uploadid,
                'block_list': json.dumps(block_list),
            }))
        except Exception:
            try:
                self.delete(remote_path)
            except Exception:
                pass
            raise
        print(f'OK: uploaded {local_path} -> {remote_path}')

    # ------------------------------------------------------------------
    # Tree (recursive) operations
    # ------------------------------------------------------------------

    def list_tree(self, remote: str = '.', max_depth: Optional[int] = None, dirs_only: bool = False) -> list:
        root_path = self.resolve_app_path(remote)
        root_meta = self.meta(root_path)
        if not root_meta:
            raise NetdiskError(f'Remote path does not exist: {remote}')
        entries = [(0, root_meta)]
        if not _is_dir(root_meta):
            return entries

        def _walk(path: str, depth: int):
            if max_depth is not None and depth >= max_depth:
                return
            for item in self.list_dir(path, dirs_only=dirs_only):
                entries.append((depth + 1, item))
                if _is_dir(item):
                    child = item.get('path') or normalize_app_path(
                        path.rstrip('/') + '/' + item.get('server_filename', '')
                    )
                    _walk(child, depth + 1)

        _walk(root_path, 0)
        return entries

    def download_tree(self, remote: str, local: str):
        m = self.meta(remote)
        if not m:
            raise NetdiskError(f'Remote path does not exist: {remote}')
        if not _is_dir(m):
            return self.download(remote, local)

        remote_path = self.resolve_app_path(remote)
        remote_name = os.path.basename(remote_path.rstrip('/')) or self.cfg.data.get('app_name', 'root')
        copy_contents = has_trailing_slash(remote) and remote != '/'

        local_path = Path(local).expanduser()
        if local_path.exists() and not local_path.is_dir():
            raise NetdiskError(f'Local path exists and is not a directory: {local_path}')

        if local_path.is_dir():
            base_local = local_path if copy_contents or local_path.name == remote_name else local_path / remote_name
        elif has_trailing_slash(local):
            local_path.mkdir(parents=True, exist_ok=True)
            base_local = local_path if copy_contents else local_path / remote_name
        else:
            base_local = local_path

        base_local.mkdir(parents=True, exist_ok=True)

        def _walk(rpath: str, lpath: Path):
            for item in self.list_dir(rpath):
                name = item['server_filename']
                child_remote = normalize_app_path(rpath.rstrip('/') + '/' + name)
                child_local = lpath / name
                if _is_dir(item):
                    child_local.mkdir(parents=True, exist_ok=True)
                    _walk(child_remote, child_local)
                else:
                    self.download(child_remote, str(child_local))

        _walk(remote_path, base_local)
        print(f'OK: downloaded directory {remote} -> {base_local}')

    def upload_tree(self, local: str, remote: str):
        local_path = Path(local).expanduser()
        if not local_path.exists():
            raise NetdiskError(f'Local path does not exist: {local}')
        if not local_path.is_dir():
            return self.upload(local, remote)

        remote_path = self.resolve_app_path(remote or '.')
        remote_meta = self.meta(remote or '.')
        src_name = local_path.name
        copy_contents = has_trailing_slash(local)

        if remote_meta and not _is_dir(remote_meta):
            raise NetdiskError(f'Remote path exists and is not a directory: {remote}')

        if remote_meta:
            base_remote = remote_path if copy_contents or os.path.basename(remote_path.rstrip('/')) == src_name \
                else normalize_app_path(remote_path.rstrip('/') + '/' + src_name)
        elif has_trailing_slash(remote) or not remote:
            self.ensure_remote_dir(remote_path)
            base_remote = remote_path if copy_contents else normalize_app_path(remote_path.rstrip('/') + '/' + src_name)
        else:
            base_remote = remote_path

        self.ensure_remote_dir(base_remote)

        for root, dirs, files in os.walk(local_path):
            rel = os.path.relpath(root, local_path)
            remote_dir = (
                base_remote if rel == '.'
                else normalize_app_path(base_remote.rstrip('/') + '/' + rel.replace(os.sep, '/'))
            )
            self.ensure_remote_dir(remote_dir)
            for fname in files:
                self.upload(str(Path(root) / fname), normalize_app_path(remote_dir.rstrip('/') + '/' + fname))

        print(f'OK: uploaded directory {local_path} -> {base_remote}')

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def quota(self) -> dict:
        data = self._json(self._get(PCS_QUOTA_URL, {'method': 'info'}))
        quota = int(data.get('quota', 0))
        used = int(data.get('used', 0))
        return {'quota': quota, 'used': used, 'free': max(0, quota - used)}

    def ping(self, count: int = 3) -> list:
        self.ensure_token()
        timings = []
        for _ in range(count):
            start = time.perf_counter()
            self.list_dir('.', limit=1)
            timings.append((time.perf_counter() - start) * 1000)
        return timings
