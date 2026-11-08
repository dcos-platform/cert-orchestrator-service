import pytest

from cert_orchestrator.models import LifecycleOperation, LifecycleState
from cert_orchestrator.state_machine import completion_required, decide_transition


def test_transition_success_completes():
    decision = decide_transition(success=True, retry_count=0, max_retries=3)

    assert decision.next_state == LifecycleState.COMPLETED
    assert decision.should_retry is False
    assert decision.should_publish_completion is True


def test_transition_failure_retries_before_max():
    decision = decide_transition(success=False, retry_count=1, max_retries=3)

    assert decision.next_state == LifecycleState.PROCESSING
    assert decision.should_retry is True
    assert decision.should_publish_completion is False


def test_transition_failure_one_below_max_still_retries():
    decision = decide_transition(success=False, retry_count=2, max_retries=3)

    assert decision.next_state == LifecycleState.PROCESSING
    assert decision.should_retry is True
    assert decision.should_publish_completion is False


def test_transition_failure_at_max_fails_and_publishes_completion():
    decision = decide_transition(success=False, retry_count=3, max_retries=3)

    assert decision.next_state == LifecycleState.FAILED
    assert decision.should_retry is False
    assert decision.should_publish_completion is True


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (LifecycleOperation.CREATE, True),
        (LifecycleOperation.RENEW, True),
        (LifecycleOperation.REVOKE, False),
        (LifecycleOperation.EXPIRE, False),
    ],
)
def test_completion_required_per_operation(operation, expected):
    assert completion_required(operation) is expected
