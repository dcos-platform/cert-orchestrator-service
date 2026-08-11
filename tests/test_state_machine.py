from cert_orchestrator.models import LifecycleState
from cert_orchestrator.state_machine import decide_transition


def test_transition_success_completes():
    decision = decide_transition(success=True, retry_count=0, max_retries=3)

    assert decision.next_state == LifecycleState.COMPLETED
    assert decision.should_retry is False
    assert decision.should_publish_completion is True


def test_transition_failure_retries_before_max():
    decision = decide_transition(success=False, retry_count=1, max_retries=3)

    assert decision.next_state == LifecycleState.FAILED
    assert decision.should_retry is True
    assert decision.should_publish_completion is False


def test_transition_failure_after_max_publishes_completion():
    decision = decide_transition(success=False, retry_count=3, max_retries=3)

    assert decision.next_state == LifecycleState.FAILED
    assert decision.should_retry is False
    assert decision.should_publish_completion is True
