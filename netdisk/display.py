import time

from .utils import _is_dir, column_format, human_size


def print_ls_long(items: list):
    for item in items:
        kind = 'd' if _is_dir(item) else '-'
        size = '-' if _is_dir(item) else human_size(int(item.get('size', 0)))
        ts = int(item.get('server_mtime') or item.get('local_mtime') or 0)
        mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(ts)) if ts else '-'
        name = item.get('server_filename', '')
        print(f'{kind} {size:>8} {mtime} {name}')


def print_ls_compact(items: list):
    names = [
        item.get('server_filename', '') + ('/' if _is_dir(item) else '')
        for item in items
    ]
    if names:
        print(column_format(names))
