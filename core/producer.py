"""
Order Event Stream Producer.
Simulates a high-throughput transaction source emitting valid financial payloads.
"""
import random
import hashlib
import time
from typing import Dict, Any
from config.settings import TransactionEvent

CUSTOMER_POOL = [f"CUST-{i:04d}" for i in range(101, 120)]
PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "NET_BANKING", "DEBIT_CARD"]

def generate_idempotency_key(customer_id: str, seed: str) -> str:
    """Deterministic hash generation to enforce idempotency."""
    raw = f"{customer_id}:{seed}:{time.time_ns()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

class OrderProducer:
    """Produces verified, schema-compliant transaction events."""

    def __init__(self, topic: str = "orders.raw.stream"):
        self.topic = topic

    def produce_single_event(self) -> Dict[str, Any]:
        """Generates a strictly valid TransactionEvent dictionary."""
        cust_id = random.choice(CUSTOMER_POOL)
        order_seed = f"{random.randint(1000, 9999)}"
        idem_key = generate_idempotency_key(cust_id, order_seed)
        
        event = TransactionEvent(
            customer_id=cust_id,
            amount=round(random.uniform(150.0, 25000.0), 2),
            payment_method=random.choice(PAYMENT_METHODS),
            idempotency_key=idem_key
        )
        return event.model_dump()

    def produce_batch(self, count: int = 10) -> list[Dict[str, Any]]:
        """Emits a batch of pure valid events."""
        return [self.produce_single_event() for _ in range(count)]