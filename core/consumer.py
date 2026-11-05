"""
Resilient Stream Consumer & Idempotency Sentinel.
Validates incoming events against strict enterprise schemas, enforces deduplication,
and transparently isolates anomalies into the Dead Letter Queue.
"""
from typing import Dict, Any, List, Set, Tuple
from pydantic import ValidationError
from config.settings import TransactionEvent
from core.dlq_manager import DeadLetterQueueManager

class ResilientConsumer:
    """Enterprise consumer enforcing idempotency and zero unhandled crash policy."""

    def __init__(self, dlq_manager: DeadLetterQueueManager):
        self.dlq = dlq_manager
        self.processed_store: List[Dict[str, Any]] = []
        self._seen_idempotency_keys: Set[str] = set()
        self.audit_log: List[Dict[str, Any]] = []

    def consume_event(self, raw_event: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Processes a single event:
        1. Checks for Idempotency
        2. Validates schema
        3. Routes to Processed Store or DLQ
        """
        # Step 1: Idempotency Key presence & uniqueness check
        idem_key = raw_event.get("idempotency_key")
        if not idem_key:
            err_msg = "Missing critical header: 'idempotency_key'"
            self.dlq.route_to_dlq(raw_event, err_msg, ValueError(err_msg))
            self.audit_log.append({"status": "REJECTED_MISSING_KEY", "payload": raw_event})
            return False, err_msg

        if idem_key in self._seen_idempotency_keys:
            err_msg = f"Duplicate event discarded: {idem_key}"
            self.audit_log.append({"status": "DUPLICATE_DISCARDED", "key": idem_key, "payload": raw_event})
            return False, err_msg

        # Step 2: Schema Validation (Pydantic deterministic validation)
        try:
            validated_event = TransactionEvent(**raw_event)
            self._seen_idempotency_keys.add(idem_key)
            self.processed_store.append(validated_event.model_dump())
            self.audit_log.append({"status": "SUCCESS_VERIFIED", "payload": validated_event.model_dump()})
            return True, "SUCCESS"
        except ValidationError as e:
            err_msg = f"Schema validation failed: {str(e.errors()[0]['msg'])}"
            self.dlq.route_to_dlq(raw_event, err_msg, e)
            self.audit_log.append({"status": "CORRUPTED_SENT_TO_DLQ", "payload": raw_event, "reason": err_msg})
            return False, err_msg

    def consume_batch(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """Processes an incoming stream batch and produces summary metrics."""
        summary = {"processed": 0, "quarantined_to_dlq": 0, "duplicates_rejected": 0}
        for ev in events:
            success, msg = self.consume_event(ev)
            if success:
                summary["processed"] += 1
            elif "Duplicate" in msg:
                summary["duplicates_rejected"] += 1
            else:
                summary["quarantined_to_dlq"] += 1
        return summary