import json
import os
from pathlib import Path
from typing import Optional


class NetdiskError(Exception):
    pass


def config_dir_path() -> Path:
    return Path.home() / '.config' / 'netdisk'


def config_file_path() -> Path:
    return config_dir_path() / 'config.json'


def load_config(path: Optional[Path] = None) -> dict:
    path = path or config_file_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def write_config(data: dict, path: Optional[Path] = None) -> None:
    if not isinstance(data, dict):
        raise NetdiskError('Config data must be a dict')

    is_default_path = path is None
    path = path or config_file_path()
    try:
        parent_existed = path.parent.exists()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if is_default_path or path == config_file_path() or not parent_existed:
            os.chmod(path.parent, 0o700)

        tmp_path = path.with_name(path.name + '.tmp')
        payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
        tmp_path.write_text(payload, encoding='utf-8')
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    except OSError as e:
        raise NetdiskError(f'Cannot write config to {path}') from e


class Config:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or config_file_path()
        self.data = load_config(self.path)

    def save(self):
        path = getattr(self, 'path', None) or config_file_path()
        write_config(self.data, path)

    def require(self, *keys):
        missing = [k for k in keys if not self.data.get(k)]
        if missing:
            raise NetdiskError(
                f'Missing config: {", ".join(missing)}. Run: netdisk login'
            )

    @property
    def app_root(self) -> str:
        self.require('app_name')
        return f"/apps/{self.data['app_name']}"

    @property
    def cwd(self) -> str:
        cwd = self.data.get('cwd', '/')
        return cwd if cwd.startswith('/') else '/' + cwd

    @cwd.setter
    def cwd(self, value: str):
        from .utils import normalize_app_path
        self.data['cwd'] = normalize_app_path(value)
