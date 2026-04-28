from types import SimpleNamespace

from netdisk import cli


class FakeClient:
    def __init__(self):
        self.calls = []

    def resolve_app_path(self, path: str) -> str:
        return '/' if path == '.' else path

    def list_dir(self, path, **kwargs):
        self.calls.append(('list_dir', path, kwargs))
        return [{'server_filename': 'dir', 'isdir': 1}]

    def copy(self, src, dst):
        self.calls.append(('copy', src, dst))

    def file_meta(self, path, **kwargs):
        self.calls.append(('file_meta', path, kwargs))
        return {'path': path, 'server_filename': 'file.txt', 'isdir': 0, 'size': 5, 'fs_id': 123}

    def search(self, keyword, **kwargs):
        self.calls.append(('search', keyword, kwargs))
        return [{'path': '/docs/report.pdf', 'server_filename': 'report.pdf', 'isdir': 0}]

    def category_list(self, category, **kwargs):
        self.calls.append(('category_list', category, kwargs))
        return [{'path': '/photos/a.jpg', 'server_filename': 'a.jpg', 'isdir': 0}]

    def list_tree(self, path='.', **kwargs):
        self.calls.append(('list_tree', path, kwargs))
        return [
            (0, {'server_filename': 'root', 'isdir': 1}),
            (1, {'server_filename': 'child.txt', 'isdir': 0}),
        ]


def _install_fake_client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(cli, '_get_client', lambda args: (SimpleNamespace(cwd='/'), fake))
    return fake


def test_ls_passes_enhanced_options(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args([
        'ls', '/target', '--sort', 'time', '--desc', '--dirs-only', '--limit', '7'
    ])
    args.func(args)
    assert fake.calls == [('list_dir', '/target', {
        'order': 'time',
        'desc': True,
        'limit': 7,
        'dirs_only': True,
    })]
    assert 'dir/' in capsys.readouterr().out


def test_cp_invokes_client_copy(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args(['cp', '/a.txt', '/b.txt'])
    args.func(args)
    assert fake.calls == [('copy', '/a.txt', '/b.txt')]
    assert 'OK: copied /a.txt -> /b.txt' in capsys.readouterr().out


def test_meta_json_output(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args(['meta', '--json', '--dlink', '/file.txt'])
    args.func(args)
    assert fake.calls == [('file_meta', '/file.txt', {
        'dlink': True,
        'thumb': False,
        'extra': False,
        'needmedia': False,
        'detail': False,
    })]
    assert '"fs_id": 123' in capsys.readouterr().out


def test_search_prints_paths(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args([
        'search', 'report', '/docs', '--recursive', '--type', 'doc', '--num', '10'
    ])
    args.func(args)
    assert fake.calls == [('search', 'report', {
        'path': '/docs',
        'recursive': True,
        'category': 'doc',
        'page': 1,
        'num': 10,
    })]
    assert '/docs/report.pdf' in capsys.readouterr().out


def test_category_prints_paths(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args(['category', 'image', '/photos', '--asc'])
    args.func(args)
    assert fake.calls == [('category_list', 'image', {
        'path': '/photos',
        'recursive': False,
        'page': 1,
        'num': 500,
        'order': 'time',
        'desc': False,
    })]
    assert '/photos/a.jpg' in capsys.readouterr().out


def test_tree_prints_indented_entries(monkeypatch, capsys):
    fake = _install_fake_client(monkeypatch)
    args = cli.build_parser().parse_args(['tree', '/', '--depth', '2'])
    args.func(args)
    assert fake.calls == [('list_tree', '/', {'max_depth': 2, 'dirs_only': False})]
    assert capsys.readouterr().out.splitlines() == ['/', '  child.txt']
