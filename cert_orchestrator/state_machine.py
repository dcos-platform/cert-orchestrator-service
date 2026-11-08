from dataclasses import dataclass

from cert_orchestrator.models import LifecycleOperation, LifecycleState


@dataclass(frozen=True)
class TransitionDecision:
    next_state: LifecycleState
    should_retry: bool
    should_publish_completion: bool


def decide_transition(*, success: bool, retry_count: int, max_retries: int) -> TransitionDecision:
    if success:
        return TransitionDecision(
            next_state=LifecycleState.COMPLETED,
            should_retry=False,
            should_publish_completion=True,
        )

    should_retry = retry_count < max_retries
    return TransitionDecision(
        next_state=LifecycleState.PROCESSING if should_retry else LifecycleState.FAILED,
        should_retry=should_retry,
        should_publish_completion=not should_retry,
    )


def completion_required(operation: LifecycleOperation) -> bool:
    """Return True when the operation is orchestrated and publishes a completion."""
    return operation in (LifecycleOperation.CREATE, LifecycleOperation.RENEW)
