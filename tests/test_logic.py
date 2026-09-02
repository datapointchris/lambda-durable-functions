"""Layer 1: pure logic, no SDK, no runner, no fakes."""

import pytest

from landing_zone.logic import build_manifest
from landing_zone.logic import is_quiet
from landing_zone.logic import next_poll_delay_seconds

NOW = 1_000_000.0


def test_a_drop_is_quiet_once_nothing_has_landed_for_the_period():
    assert is_quiet(NOW - 300, NOW, quiet_seconds=300) is True


def test_a_recent_arrival_keeps_the_drop_open():
    assert is_quiet(NOW - 299, NOW, quiet_seconds=300) is False


def test_an_empty_prefix_is_never_quiet():
    assert is_quiet(None, NOW, quiet_seconds=300) is False


def test_manifest_keys_are_sorted_and_bytes_summed():
    manifest = build_manifest('run-1', {'landing/b.csv': 20, 'landing/a.csv': 10})
    assert manifest.keys == ('landing/a.csv', 'landing/b.csv')
    assert manifest.total_bytes == 30


def test_an_empty_drop_yields_an_empty_manifest():
    manifest = build_manifest('run-1', {})
    assert manifest.keys == ()
    assert manifest.total_bytes == 0


def test_the_first_attempt_uses_schedule_element_zero():
    assert next_poll_delay_seconds(1, schedule=(60, 120), steady=300) == 60


@pytest.mark.parametrize(('attempt', 'expected'), [(2, 120), (3, 300), (9, 300)])
def test_later_attempts_walk_the_schedule_then_hold_steady(attempt, expected):
    assert next_poll_delay_seconds(attempt, schedule=(60, 120), steady=300) == expected
