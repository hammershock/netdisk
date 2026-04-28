import time

from .utils import _is_dir, column_format, human_size


def item_name(item: dict) -> str:
    name = item.get('server_filename') or item.get('filename')
    if name:
        return name
    path = item.get('path') or ''
    return path.rstrip('/').rsplit('/', 1)[-1] or path or ''


def item_path(item: dict) -> str:
    path = item.get('path')
    if path:
        return path
    return item_name(item)


def print_ls_long(items: list):
    for item in items:
        kind = 'd' if _is_dir(item) else '-'
        size = '-' if _is_dir(item) else human_size(int(item.get('size', 0)))
        ts = int(item.get('server_mtime') or item.get('local_mtime') or 0)
        mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(ts)) if ts else '-'
        name = item_name(item)
        print(f'{kind} {size:>8} {mtime} {name}')


def print_ls_compact(items: list):
    names = [
        item_name(item) + ('/' if _is_dir(item) else '')
        for item in items
    ]
    if names:
        print(column_format(names))


def print_path_list(items: list, long: bool = False):
    for item in items:
        path = item_path(item)
        if not long:
            print(path + ('/' if _is_dir(item) and not path.endswith('/') else ''))
            continue
        kind = 'd' if _is_dir(item) else '-'
        size = '-' if _is_dir(item) else human_size(int(item.get('size', 0)))
        ts = int(item.get('server_mtime') or item.get('local_mtime') or 0)
        mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(ts)) if ts else '-'
        print(f'{kind} {size:>8} {mtime} {path}')
