"""Tab-completion for the interactive shell.

Completion is context-aware:
- First token  → subcommand name
- Later tokens → remote path, local path, or nothing, based on the command
                 and how many positional arguments have been typed so far.

Remote directory listings are cached for _CACHE_TTL seconds so that rapid
tab presses don't hammer the API.

Space-in-name handling
----------------------
readline splits `text` on its delimiter set (space/tab/newline).  If a
directory name contains a space, e.g. "Obsidian Vault", readline hands us
only the tail ("Vault") as `text`.  We work around this by:

1. Using `readline.get_line_buffer()` + `shlex.split()` to recover the
   *full* current argument (the actual filesystem/remote path the user
   intends).  This is used for directory listing and name filtering.

2. Using `text` (readline's view) only to reconstruct the replacement
   string readline will paste back, keeping its dir-prefix intact.

3. Escaping spaces in returned completions with a backslash so readline
   inserts e.g. `Obsidian\ Vault/`, which shlex.split() later re-unifies
   into `Obsidian Vault/`.
"""
import os
import shlex
import time
from typing import Callable, List, Optional

try:
    import readline
    _HAS_READLINE = True
except ImportError:
    _HAS_READLINE = False

# Completion type per positional argument index.
# The last entry is repeated for variadic commands (e.g. rm accepts N paths).
_CMD_ARG_TYPES: dict = {
    'ls':       ('remote',),
    'cd':       ('remote',),
    'mkdir':    ('remote',),
    'rm':       ('remote',),
    'mv':       ('remote', 'remote'),
    'cp':       ('remote', 'remote'),
    'meta':     ('remote',),
    'stat':     ('remote',),
    'tree':     ('remote',),
    'search':   ('none', 'remote'),
    'category': ('none', 'remote'),
    'download': ('remote', 'local'),
    'upload':   ('local',  'remote'),
    'pwd':      (),
    'ping':     (),
    'quota':    (),
    'login':    (),
    'help':     (),
    'exit':     (),
    'quit':     (),
}

_ALL_COMMANDS: List[str] = sorted(_CMD_ARG_TYPES)

# Commands whose last positional type repeats for any number of arguments.
_VARIADIC_COMMANDS = {'rm'}

_CACHE_TTL = 30  # seconds


def _escape(name: str) -> str:
    """Escape characters readline treats as word delimiters."""
    return name.replace(' ', r'\ ')


class ShellCompleter:
    """Readline completer for the netdisk interactive shell."""

    def __init__(self, get_client: Callable):
        """
        get_client: zero-arg callable that returns a BaiduNetdiskClient.
        Called lazily, only when a remote path completion is needed.
        """
        self._get_client = get_client
        self._remote_cache: dict = {}  # path -> list[dict]
        self._cache_ts: dict = {}      # path -> float (monotonic)
        self._matches: List[str] = []

    def install(self) -> None:
        """Register with readline. No-op if readline is not available."""
        if not _HAS_READLINE:
            return
        # Use only whitespace as delimiters so '/' is kept inside `text`.
        readline.set_completer_delims(' \t\n')
        readline.set_completer(self.complete)
        readline.parse_and_bind('tab: complete')

    def invalidate(self, path: Optional[str] = None) -> None:
        """Drop cached listing for `path` (or all if None)."""
        if path is None:
            self._remote_cache.clear()
            self._cache_ts.clear()
        else:
            self._remote_cache.pop(path, None)
            self._cache_ts.pop(path, None)

    def prewarm(self, path: str) -> None:
        """Fetch and cache the listing for `path` in a background thread."""
        import threading
        threading.Thread(target=self._list_remote, args=(path,), daemon=True).start()

    # ------------------------------------------------------------------
    # readline entry point
    # ------------------------------------------------------------------

    def complete(self, text: str, state: int) -> Optional[str]:
        if state == 0:
            try:
                self._matches = self._compute(text)
            except Exception:
                self._matches = []
        return self._matches[state] if state < len(self._matches) else None

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def _compute(self, text: str) -> List[str]:
        if not _HAS_READLINE:
            return []

        line = readline.get_line_buffer()

        # Split only the text before the cursor so unterminated quotes
        # (e.g. upload "My Dir<TAB>) don't corrupt the token list.
        # tokens never contains the word being typed (that is `text`).
        begidx = readline.get_begidx()
        prefix_line = line[:begidx]
        try:
            tokens = shlex.split(prefix_line)
        except ValueError:
            tokens = prefix_line.split()

        # ── Completing the command name ────────────────────────────────
        # tokens is derived from prefix_line (before cursor), so it never
        # contains the word being typed. If tokens is empty (or only the
        # command name itself hasn't been committed yet), complete commands.
        if not tokens:
            return [c + ' ' for c in _ALL_COMMANDS if c.startswith(text)]

        cmd = tokens[0]
        arg_types = _CMD_ARG_TYPES.get(cmd)
        if not arg_types:
            return []

        # Count completed positionals before the cursor (skip flags).
        # tokens comes from prefix_line and never contains the word being
        # typed, so len(positional) is exactly the index of the current arg.
        positional = [t for t in tokens[1:] if not t.startswith('-')]
        pos_idx = len(positional)

        # Stop completing once all positionals are filled, unless variadic.
        if pos_idx >= len(arg_types) and cmd not in _VARIADIC_COMMANDS:
            return []
        comp_type = arg_types[min(pos_idx, len(arg_types) - 1)]

        # full_arg: the word currently being typed (readline's `text`).
        # prefix_line / tokens never contain this word, so we always use text.
        full_arg = text

        if comp_type == 'remote':
            return self._remote(full_arg, text)
        if comp_type == 'local':
            return self._local(full_arg, text)
        return []

    # ------------------------------------------------------------------
    # Remote path completion
    # ------------------------------------------------------------------

    def _remote(self, full_arg: str, rl_text: str) -> List[str]:
        """
        full_arg  – shlex-parsed full path (used to list the right directory)
        rl_text   – readline's `text` (used to construct the replacement string)
        """
        if '/' in full_arg:
            full_dir, name_part = full_arg.rsplit('/', 1)
            list_path = full_dir or '/'
        else:
            list_path = '.'
            name_part = full_arg

        # Prefix for the replacement string: whatever readline sees up to
        # the last slash in rl_text (preserves already-typed dir component).
        if '/' in rl_text:
            rl_prefix = rl_text.rsplit('/', 1)[0] + '/'
        else:
            rl_prefix = ''

        items = self._list_remote(list_path)
        matches = []
        for item in items:
            name: str = item.get('server_filename', '')
            if not name.startswith(name_part):
                continue
            is_dir = int(item.get('isdir', 0)) == 1
            matches.append(rl_prefix + _escape(name) + ('/' if is_dir else ''))
        return matches

    def _list_remote(self, path: str) -> list:
        # Normalize to absolute path so the cache key is stable across cd.
        # prewarm() stores under the absolute path; Tab completion must use the
        # same key to get a cache hit.
        try:
            client = self._get_client()
            path = client.resolve_app_path(path)
        except Exception:
            return []
        now = time.monotonic()
        if path in self._remote_cache and now - self._cache_ts.get(path, 0) < _CACHE_TTL:
            return self._remote_cache[path]
        try:
            items = client.list_dir(path)
            self._remote_cache[path] = items
            self._cache_ts[path] = now
            return items
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Local path completion
    # ------------------------------------------------------------------

    def _local(self, full_arg: str, rl_text: str) -> List[str]:
        """
        full_arg  – shlex-parsed full local path
        rl_text   – readline's `text`
        """
        home = os.path.expanduser('~')
        uses_tilde = full_arg.startswith('~')

        # Special-case bare '~' or '~/': list the home directory directly.
        # Without this, expanduser('~') = '/Users/foo' is split at the last '/'
        # into dir='/Users' + name='foo', which lists /Users and matches 'foo'
        # rather than listing the home dir.
        if full_arg in ('~', '~/'):
            expanded_dir = home
            name_part = ''
            rl_prefix = '~/'
        else:
            expanded = os.path.expanduser(full_arg)
            if '/' in expanded:
                expanded_dir, name_part = expanded.rsplit('/', 1)
                expanded_dir = expanded_dir or '/'
                if uses_tilde and expanded_dir.startswith(home):
                    rl_prefix = '~' + expanded_dir[len(home):] + '/'
                elif '/' in rl_text:
                    rl_prefix = rl_text.rsplit('/', 1)[0] + '/'
                else:
                    rl_prefix = ''
            else:
                expanded_dir = '.'
                name_part = expanded
                rl_prefix = ''

        try:
            entries = os.listdir(expanded_dir)
        except OSError:
            return []

        matches = []
        for entry in entries:
            if not entry.startswith(name_part):
                continue
            full_path = os.path.join(expanded_dir, entry)
            suffix = '/' if os.path.isdir(full_path) else ''
            matches.append(rl_prefix + _escape(entry) + suffix)
        return matches
