"""
Dead Letter Queue (DLQ) Manager.
Isolates corrupted, malformed, or unprocessable events into a dedicated
recovery topic to ensure zero data loss and enable post-incident replays.
"""
from typing import List, Dict, Any
from config.settings import DLQEnvelope, TOPIC_DEAD_LETTER_QUEUE

class DeadLetterQueueManager:
    """Enterprise-grade quarantine handler for poisoned messages."""

    def __init__(self, dlq_topic: str = TOPIC_DEAD_LETTER_QUEUE):
        self.dlq_topic = dlq_topic
        self._quarantine: List[Dict[str, Any]] = []

    def route_to_dlq(self, payload: Dict[str, Any], reason: str, exc: Exception) -> Dict[str, Any]:
        """Wraps corrupted payload into a formal DLQ envelope and stores it."""
        envelope = DLQEnvelope(
            original_payload=payload,
            failure_reason=reason,
            exception_type=exc.__class__.__name__
        )
        record = envelope.model_dump()
        self._quarantine.append(record)
        return record

    def get_all_quarantined(self) -> List[Dict[str, Any]]:
        """Returns snapshot of current quarantined records."""
        return list(self._quarantine)

    def purge(self) -> None:
        """Clears quarantine buffer."""
        self._quarantine.clear()

    @property
    def count(self) -> int:
        return len(self._quarantine)