import pytest

from netdisk.utils import (
    _is_dir,
    column_format,
    format_seconds,
    has_trailing_slash,
    human_size,
    normalize_app_path,
)


class TestNormalizeAppPath:
    def test_empty_returns_root(self):
        assert normalize_app_path('') == '/'

    def test_root_unchanged(self):
        assert normalize_app_path('/') == '/'

    def test_absolute_path(self):
        assert normalize_app_path('/foo/bar') == '/foo/bar'

    def test_relative_path_becomes_absolute(self):
        assert normalize_app_path('foo/bar') == '/foo/bar'

    def test_trailing_slash_stripped(self):
        assert normalize_app_path('/foo/bar/') == '/foo/bar'

    def test_dot_segments_removed(self):
        assert normalize_app_path('/foo/./bar') == '/foo/bar'

    def test_dotdot_collapses_parent(self):
        assert normalize_app_path('/a/b/../c') == '/a/c'

    def test_dotdot_at_root_stays_root(self):
        assert normalize_app_path('/..') == '/'

    def test_multiple_slashes_collapsed(self):
        assert normalize_app_path('/foo//bar') == '/foo/bar'

    def test_deep_dotdot(self):
        assert normalize_app_path('/a/b/c/../../d') == '/a/d'

    def test_only_dotdot_from_relative(self):
        assert normalize_app_path('..') == '/'


class TestHasTrailingSlash:
    def test_empty_string_false(self):
        assert not has_trailing_slash('')

    def test_root_slash_true(self):
        assert has_trailing_slash('/')

    def test_path_with_trailing_slash(self):
        assert has_trailing_slash('foo/')

    def test_path_without_trailing_slash(self):
        assert not has_trailing_slash('foo')

    def test_absolute_with_trailing_slash(self):
        assert has_trailing_slash('/foo/bar/')

    def test_absolute_without_trailing_slash(self):
        assert not has_trailing_slash('/foo/bar')


class TestIsDir:
    def test_isdir_one_is_directory(self):
        assert _is_dir({'isdir': 1})

    def test_isdir_zero_is_not_directory(self):
        assert not _is_dir({'isdir': 0})

    def test_missing_key_defaults_to_not_directory(self):
        assert not _is_dir({})

    def test_isdir_string_one(self):
        assert _is_dir({'isdir': '1'})

    def test_isdir_string_zero(self):
        assert not _is_dir({'isdir': '0'})


class TestHumanSize:
    def test_zero_bytes(self):
        assert human_size(0) == '0B'

    def test_bytes(self):
        assert human_size(512) == '512B'

    def test_exactly_one_kb(self):
        assert human_size(1024) == '1.0K'

    def test_fractional_kb(self):
        assert human_size(1536) == '1.5K'

    def test_exactly_one_mb(self):
        assert human_size(1024 ** 2) == '1.0M'

    def test_exactly_one_gb(self):
        assert human_size(1024 ** 3) == '1.0G'

    def test_exactly_one_tb(self):
        assert human_size(1024 ** 4) == '1.0T'


class TestFormatSeconds:
    def test_none_returns_placeholder(self):
        assert format_seconds(None) == '--:--'

    def test_negative_returns_placeholder(self):
        assert format_seconds(-1) == '--:--'

    def test_inf_returns_placeholder(self):
        assert format_seconds(float('inf')) == '--:--'

    def test_zero(self):
        assert format_seconds(0) == '00:00'

    def test_under_one_minute(self):
        assert format_seconds(45) == '00:45'

    def test_one_minute(self):
        assert format_seconds(60) == '01:00'

    def test_one_hour_five_minutes_six_seconds(self):
        assert format_seconds(3906) == '01:05:06'

    def test_just_under_one_hour(self):
        assert format_seconds(3599) == '59:59'

    def test_exactly_one_hour(self):
        assert format_seconds(3600) == '01:00:00'


class TestColumnFormat:
    def test_empty_list_returns_empty_string(self):
        assert column_format([]) == ''

    def test_single_item(self):
        result = column_format(['file.txt'])
        assert 'file.txt' in result

    def test_multiple_items_all_present(self):
        names = ['alpha', 'beta', 'gamma']
        result = column_format(names)
        for name in names:
            assert name in result

    def test_no_extra_trailing_whitespace_per_line(self):
        names = ['a', 'bb', 'ccc']
        for line in column_format(names).splitlines():
            assert line == line.rstrip()
