"""What DurableFunctionTestRunner cannot do, pinned as executable tests.

Both are properties of the harness rather than of the SDK. They decide how the
rest of the suite is written, so they are asserted rather than written down.
"""

import time

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig
from aws_durable_execution_sdk_python.waits import WaitForConditionDecision
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

SEEN: list[dict] = []


@durable_execution
def counting_handler(_event: dict, context: DurableContext) -> dict:
    def check(state, _check_context):
        SEEN.append(dict(state))
        return {'n': state['n'] + 1}

    def strategy(state, _attempt):
        if state['n'] >= 3:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    return context.wait_for_condition(
        check=check,
        config=WaitForConditionConfig(wait_strategy=strategy, initial_state={'n': 0}),
        name='count',
    )


@pytest.mark.xfail(
    reason=(
        'testing 1.2.1 drops wait_for_condition polling state. The SDK sends the '
        'serialized state as OperationUpdate.payload on RETRY; the in-memory step '
        'processor never reads it and copies the previous step_details.result, '
        'which is None. Every check therefore receives initial_state.'
    ),
    strict=True,
)
def test_wait_for_condition_threads_state_between_attempts():
    SEEN.clear()
    with DurableFunctionTestRunner(counting_handler) as runner:
        runner.run(input='{}', timeout=15)

    assert [s['n'] for s in SEEN] == [0, 1, 2]


def test_a_modeled_wait_costs_real_wall_clock_time():
    """No SkipClock in 1.2.1, so handler durations have to be injectable."""

    @durable_execution
    def waits(_event: dict, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(3), name='long')
        return 'done'

    start = time.monotonic()
    with DurableFunctionTestRunner(waits) as runner:
        runner.run(input='{}', timeout=30)
    assert time.monotonic() - start >= 3
