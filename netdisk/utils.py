import shutil
import sys
import time
from typing import Optional

import requests

from .constants import PROGRESS_REFRESH_INTERVAL


def normalize_app_path(path: str) -> str:
    if not path:
        return '/'
    parts = []
    for part in path.split('/'):
        if part in ('', '.'):
            continue
        if part == '..':
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return '/' + '/'.join(parts)


def has_trailing_slash(raw_path: str) -> bool:
    return bool(raw_path) and raw_path.endswith('/')


def _is_dir(item: dict) -> bool:
    return int(item.get('isdir', 0)) == 1


def human_size(num: float) -> str:
    units = ['B', 'K', 'M', 'G', 'T']
    x = float(num)
    for unit in units:
        if x < 1024 or unit == units[-1]:
            return f'{x:.0f}{unit}' if unit == 'B' else f'{x:.1f}{unit}'
        x /= 1024
    return f'{num}B'


def format_seconds(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0 or seconds == float('inf'):
        return '--:--'
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f'{h:02d}:{m:02d}:{s:02d}'
    return f'{m:02d}:{s:02d}'


class ProgressPrinter:
    def __init__(self, label: str, total: Optional[int]):
        self.label = label
        self.total = total if total is not None and total >= 0 else None
        self.start_time = time.time()
        self.last_render_time = 0.0
        self.current = 0
        self.finished = False
        self.last_len = 0
        self.is_tty = sys.stderr.isatty()

    def update(self, current: int, force: bool = False):
        self.current = max(0, current)
        now = time.time()
        if not force and now - self.last_render_time < PROGRESS_REFRESH_INTERVAL:
            return
        self.last_render_time = now
        self._render(now)

    def advance(self, delta: int, force: bool = False):
        self.update(self.current + delta, force=force)

    def finish(self):
        if self.finished:
            return
        if self.total is not None:
            self.current = self.total
        self._render(time.time(), final=True, force=True)
        if self.is_tty:
            sys.stderr.write('\n')
            sys.stderr.flush()
        self.finished = True

    def _line(self, now: float, final: bool = False) -> str:
        elapsed = max(now - self.start_time, 1e-6)
        rate = self.current / elapsed
        eta = None
        percent_text = '--.-%'
        progress_text = human_size(self.current)
        bar = ''
        if self.total is not None:
            done = min(self.current, self.total)
            ratio = 1.0 if self.total == 0 else max(0.0, min(1.0, done / self.total))
            filled = int(ratio * 24)
            bar = '[' + '#' * filled + '-' * (24 - filled) + '] '
            percent_text = f'{ratio * 100:5.1f}%'
            progress_text = f'{human_size(done)}/{human_size(self.total)}'
            if rate > 0 and done < self.total:
                eta = (self.total - done) / rate
            elif done >= self.total:
                eta = 0
        speed_text = f'{human_size(rate)}/s' if rate > 0 else '--.-/s'
        line = (
            f'{self.label} {bar}{percent_text} {progress_text} {speed_text} '
            f'elapsed:{format_seconds(elapsed)} eta:{format_seconds(eta)}'
        )
        if final:
            line += ' done'
        return line

    def _render(self, now: float, final: bool = False, force: bool = False):
        line = self._line(now, final=final)
        if self.is_tty:
            pad = ' ' * max(0, self.last_len - len(line))
            sys.stderr.write('\r' + line + pad)
            sys.stderr.flush()
            self.last_len = len(line)
        elif force or final:
            sys.stderr.write(line + '\n')
            sys.stderr.flush()


def make_session(via_proxy: bool = False) -> requests.Session:
    session = requests.Session()
    if via_proxy:
        session.trust_env = True
    else:
        session.trust_env = False
        session.proxies.update({'http': None, 'https': None})
    return session


def column_format(names: list) -> str:
    if not names:
        return ''
    term_width = shutil.get_terminal_size((100, 20)).columns
    width = max(len(x) for x in names) + 2
    cols = max(1, term_width // max(1, width))
    rows = (len(names) + cols - 1) // cols
    lines = []
    for r in range(rows):
        row = []
        for c in range(cols):
            idx = c * rows + r
            if idx < len(names):
                row.append(names[idx].ljust(width))
        lines.append(''.join(row).rstrip())
    return '\n'.join(lines)
