import argparse
import os
import shlex
import sys
import time
import urllib.parse
import webbrowser

import requests

from .client import BaiduNetdiskClient
from .config import Config, NetdiskError
from .constants import AUTHORIZE_URL, TOKEN_URL
from .display import print_ls_compact, print_ls_long
from .utils import make_session, normalize_app_path


# When running inside the interactive shell, holds the in-memory cwd.
# None means we are in single-command mode; use Config().cwd as usual.
_shell_cwd: list = []  # list of one str, so it's mutable from nested scopes


def _get_client(args) -> tuple:
    cfg = Config()
    if _shell_cwd:
        cfg.cwd = _shell_cwd[0]  # override in-memory only, never saved
    client = BaiduNetdiskClient(cfg, via_proxy=getattr(args, 'via_proxy', False))
    return cfg, client


# ------------------------------------------------------------------
# Command handlers
# ------------------------------------------------------------------

def do_login(args):
    cfg = Config()
    client_id = getattr(args, 'app_key', None) or getattr(args, 'client_id', None)
    if not client_id:
        print('登录百度网盘开放平台创建应用获取AppKey和SecretKey')
        client_id = input('AppKey: ').strip()
    client_secret = getattr(args, 'secretkey', None) or getattr(args, 'client_secret', None) or input('Secretkey: ').strip()
    app_name = args.app_name or input('App name: ').strip()

    params = {
        'response_type': 'code',
        'client_id': client_id,
        'redirect_uri': 'oob',
        'scope': 'basic,netdisk',
        'display': 'popup',
        'force_login': 1,
    }
    auth_url = AUTHORIZE_URL + '?' + urllib.parse.urlencode(params)
    print('Open this URL in your browser, authorize the app, then paste the returned code:\n')
    print(auth_url)
    print()
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    code = args.code or input('code = ').strip()
    session = make_session(via_proxy=getattr(args, 'via_proxy', False))
    resp = session.get(TOKEN_URL, params={
        'grant_type': 'authorization_code',
        'code': code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': 'oob',
    }, timeout=30)
    data = resp.json()
    if 'access_token' not in data:
        raise NetdiskError(f'Login failed: {data}')

    cfg.data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'app_name': app_name,
        'refresh_token': data['refresh_token'],
        'access_token': data['access_token'],
        'expires_at': time.time() + int(data.get('expires_in', 86400)),
        'cwd': '/',
    }
    cfg.save()
    print(f'OK: logged in, remote root is /apps/{app_name}')


def do_ls(args):
    cfg, client = _get_client(args)
    if args.cwd:
        print(cfg.cwd)
        return
    items = client.list_dir(args.path or '.')
    if args.long:
        print_ls_long(items)
    else:
        print_ls_compact(items)


def do_cd(args):
    cfg, client = _get_client(args)
    target = args.path
    app_path = (
        normalize_app_path(target) if target.startswith('/')
        else normalize_app_path(cfg.cwd.rstrip('/') + '/' + target)
    )
    meta = client.meta(app_path)
    if not meta or not int(meta.get('isdir', 0)):
        raise NetdiskError(f'Not a directory: {target}')
    if _shell_cwd:
        _shell_cwd[0] = app_path   # interactive: update in-memory only
    else:
        cfg.cwd = app_path
        cfg.save()
    print(f'OK: changed directory to {app_path}')


def do_pwd(args):
    print(Config().cwd)


def do_mkdir(args):
    cfg, client = _get_client(args)
    client.mkdir(args.path)
    print(f'OK: created directory {args.path}')


def do_upload(args):
    _, client = _get_client(args)
    client.upload_tree(args.local, args.remote)


def _default_download_dir() -> str:
    """Return the OS download folder, respecting XDG on Linux."""
    # XDG user dirs (Linux and some BSDs)
    xdg_config = os.path.join(os.path.expanduser('~'), '.config', 'user-dirs.dirs')
    if os.path.isfile(xdg_config):
        try:
            with open(xdg_config) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('XDG_DOWNLOAD_DIR='):
                        value = line.split('=', 1)[1].strip().strip('"')
                        value = value.replace('$HOME', os.path.expanduser('~'))
                        if value:
                            return value
        except OSError:
            pass
    return os.path.join(os.path.expanduser('~'), 'Downloads')


def do_download(args):
    _, client = _get_client(args)
    local = args.local if args.local is not None else _default_download_dir()
    client.download_tree(args.remote, local)


def do_rm(args):
    _, client = _get_client(args)
    failed = False
    for raw_path in args.paths:
        try:
            app_path = client.resolve_app_path(raw_path)
            if app_path == '/':
                raise NetdiskError('Refusing to delete app root / for safety')
            meta = client.meta(app_path)
            if not meta:
                if args.force:
                    continue
                raise NetdiskError(f'Remote path does not exist: {raw_path}')
            if int(meta.get('isdir', 0)) == 1 and not args.recursive:
                raise NetdiskError(f'{raw_path} is a directory; use netdisk rm -r {raw_path}')
            client.delete(app_path)
            print(f'OK: removed {raw_path}')
        except NetdiskError as e:
            print(f'Error: {e}', file=sys.stderr)
            failed = True
    if failed:
        sys.exit(1)


def do_mv(args):
    _, client = _get_client(args)
    client.move(args.src, args.dst)
    print(f'OK: moved {args.src} -> {args.dst}')


def do_ping(args):
    if args.count < 1:
        raise NetdiskError('--count must be >= 1')
    cfg, client = _get_client(args)
    timings = client.ping(count=args.count)
    via = 'via proxy' if getattr(args, 'via_proxy', False) else 'direct'
    print(f'PING netdisk api ({via}, cwd={cfg.cwd})')
    for idx, ms in enumerate(timings, start=1):
        print(f'{idx}: {ms:.1f} ms')
    print(f'min/avg/max = {min(timings):.1f}/{sum(timings) / len(timings):.1f}/{max(timings):.1f} ms')


def do_quota(args):
    _, client = _get_client(args)
    from .utils import human_size
    q = client.quota()
    used_pct = q['used'] * 100.0 / max(1, q['quota'])
    print('Filesystem  Size  Used  Avail  Use%  Mounted on')
    print(f"netdisk     {human_size(q['quota']):>5} {human_size(q['used']):>5} {human_size(q['free']):>5} {used_pct:>4.0f}%  /")


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog='netdisk', description='Baidu Netdisk CLI')
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--via-proxy', action='store_true',
                        help='Use proxy from environment variables for this command')
    sub = p.add_subparsers(dest='command', required=True)

    sp = sub.add_parser('login', help='Log in and save credentials', parents=[common])
    sp.add_argument('--app-key', dest='app_key', help='AppKey from Baidu Netdisk Open Platform')
    sp.add_argument('--secretkey', dest='secretkey', help='Secretkey from Baidu Netdisk Open Platform')
    sp.add_argument('--client-id', dest='client_id', help=argparse.SUPPRESS)
    sp.add_argument('--client-secret', dest='client_secret', help=argparse.SUPPRESS)
    sp.add_argument('--app-name')
    sp.add_argument('--code')
    sp.set_defaults(func=do_login)

    sp = sub.add_parser('ls', parents=[common], help='List remote directory')
    sp.add_argument('path', nargs='?', default='.')
    sp.add_argument('-l', '--long', action='store_true', help='Long listing format')
    sp.add_argument('--cwd', action='store_true', help='Print current remote working directory')
    sp.set_defaults(func=do_ls)

    sp = sub.add_parser('cd', parents=[common], help='Change remote working directory')
    sp.add_argument('path')
    sp.set_defaults(func=do_cd)

    sp = sub.add_parser('pwd', parents=[common], help='Print remote working directory')
    sp.set_defaults(func=do_pwd)

    sp = sub.add_parser('mkdir', parents=[common], help='Create remote directory')
    sp.add_argument('path')
    sp.set_defaults(func=do_mkdir)

    sp = sub.add_parser('upload', parents=[common], help='Upload file or directory')
    sp.add_argument('local')
    sp.add_argument('remote', nargs='?', default='.')
    sp.set_defaults(func=do_upload)

    sp = sub.add_parser('download', parents=[common], help='Download file or directory')
    sp.add_argument('remote')
    sp.add_argument('local', nargs='?', default=None,
                    help='Local destination (default: ~/Downloads on macOS/Linux)')
    sp.set_defaults(func=do_download)

    sp = sub.add_parser('rm', parents=[common], help='Remove remote file or directory')
    sp.add_argument('-r', '--recursive', action='store_true', help='Recursively remove directories')
    sp.add_argument('-f', '--force', action='store_true', help='Ignore nonexistent paths')
    sp.add_argument('paths', nargs='+')
    sp.set_defaults(func=do_rm)

    sp = sub.add_parser('mv', parents=[common], help='Move or rename remote file or directory')
    sp.add_argument('src')
    sp.add_argument('dst')
    sp.set_defaults(func=do_mv)

    sp = sub.add_parser('ping', parents=[common], help='Measure API latency')
    sp.add_argument('-c', '--count', type=int, default=3, metavar='N',
                    help='Number of requests, must be >= 1 (default: 3)')
    sp.set_defaults(func=do_ping)

    sp = sub.add_parser('quota', parents=[common], help='Show disk usage')
    sp.set_defaults(func=do_quota)

    return p


_HISTORY_FILE = None
try:
    from pathlib import Path as _Path
    _HISTORY_FILE = _Path.home() / '.netdisk_history'
except Exception:
    pass


def _run_interactive():
    from ._completion import ShellCompleter

    # Interactive shell uses its own in-memory cwd starting at /.
    # Config().cwd (the persistent cwd used by single commands) is never touched.
    _shell_cwd.append('/')

    def _make_client():
        cfg = Config()
        cfg.cwd = _shell_cwd[0]
        return BaiduNetdiskClient(cfg)

    completer = ShellCompleter(_make_client)
    completer.install()
    completer.prewarm('/')  # warm root so first Tab is instant

    try:
        import readline
        if _HISTORY_FILE:
            try:
                readline.read_history_file(_HISTORY_FILE)
            except FileNotFoundError:
                pass
            import atexit
            atexit.register(readline.write_history_file, _HISTORY_FILE)
    except ImportError:
        pass

    parser = build_parser()
    print("netdisk shell — type 'help' for commands, 'exit' or Ctrl-D to quit.")

    try:
        while True:
            try:
                line = input(f'netdisk:{_shell_cwd[0]}> ').strip()
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                continue

            if not line:
                continue
            if line in ('exit', 'quit'):
                break
            if line == 'help':
                parser.print_help()
                continue

            try:
                tokens = shlex.split(line)
            except ValueError as e:
                print(f'Error: {e}', file=sys.stderr)
                continue

            try:
                args = parser.parse_args(tokens)
                args.func(args)
                cwd = _shell_cwd[0]
                if tokens[0] in ('ls', 'cd'):
                    completer.prewarm(cwd)
                elif tokens[0] in ('mkdir', 'rm', 'mv', 'upload', 'login'):
                    completer.invalidate()
                    completer.prewarm(cwd)
            except SystemExit:
                pass  # argparse already printed the error message
            except KeyboardInterrupt:
                print()
            except NetdiskError as e:
                print(f'Error: {e}', file=sys.stderr)
            except requests.RequestException as e:
                print(f'Network error: {e}', file=sys.stderr)
    finally:
        _shell_cwd.clear()  # restore single-command mode


def main():
    if not sys.argv[1:]:
        if sys.stdin.isatty():
            _run_interactive()
        else:
            build_parser().print_help()
        return

    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print('\nCancelled', file=sys.stderr)
        sys.exit(130)
    except NetdiskError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f'Network error: {e}', file=sys.stderr)
        sys.exit(2)
