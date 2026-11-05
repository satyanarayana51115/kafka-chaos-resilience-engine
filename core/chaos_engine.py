"""
Chaos & Fault Injection Engine.
Deliberately mutates valid event streams to simulate distributed failures,
schema corruptions, network retries, and duplicate delivery anomalies.
"""
import copy
import random
from typing import List, Dict, Any, Tuple

class ChaosMutationEngine:
    """Simulates real-world distributed messaging anomalies."""

    def __init__(
        self,
        corruption_rate: float = 0.15,
        duplicate_rate: float = 0.10,
        missing_field_rate: float = 0.10
    ):
        self.corruption_rate = corruption_rate
        self.duplicate_rate = duplicate_rate
        self.missing_field_rate = missing_field_rate

    def inject_chaos(self, events: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        Takes clean events, applies chaos mutations, and returns
        the disrupted stream along with mutation telemetry.
        """
        disrupted_stream: List[Dict[str, Any]] = []
        metrics = {
            "clean_events": 0,
            "corrupted_amounts": 0,
            "duplicate_events": 0,
            "missing_keys": 0
        }

        for ev in events:
            ev_copy = copy.deepcopy(ev)
            dice = random.random()

            # 1. Fault: Missing Idempotency Key or Critical Field
            if dice < self.missing_field_rate:
                ev_copy.pop("idempotency_key", None)
                disrupted_stream.append(ev_copy)
                metrics["missing_keys"] += 1

            # 2. Fault: Schema & Amount Corruption
            elif dice < (self.missing_field_rate + self.corruption_rate):
                # Invalidate amount (make it negative or wrong type)
                ev_copy["amount"] = -1 * abs(ev_copy.get("amount", 100.0))
                ev_copy["currency"] = "INVALID_CURR"
                disrupted_stream.append(ev_copy)
                metrics["corrupted_amounts"] += 1

            # Clean Event Path
            else:
                disrupted_stream.append(ev_copy)
                metrics["clean_events"] += 1

            # 3. Fault: Duplicate Injection (Simulating at-least-once network retry)
            if random.random() < self.duplicate_rate:
                duplicate_payload = copy.deepcopy(ev)
                disrupted_stream.append(duplicate_payload)
                metrics["duplicate_events"] += 1

        return disrupted_stream, metrics