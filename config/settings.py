"""
Event Stream Configuration and Pydantic Data Models.
Defines valid transaction schemas, Kafka topic constants, and chaos simulation thresholds.
"""
from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from datetime import datetime
import uuid

# Kafka Topics Constants
TOPIC_INCOMING_ORDERS = "orders.raw.stream"
TOPIC_PROCESSED_ORDERS = "orders.verified.stream"
TOPIC_DEAD_LETTER_QUEUE = "orders.dlq.failed"

class TransactionEvent(BaseModel):
    """Deterministic Enterprise Transaction Schema."""
    order_id: str = Field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:8].upper()}")
    customer_id: str
    amount: float = Field(gt=0, description="Amount must be strictly positive")
    currency: str = Field(default="INR")
    payment_method: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    idempotency_key: str

    class Config:
        validate_assignment = True

class DLQEnvelope(BaseModel):
    """Dead Letter Queue Envelope to capture failed messages with full context."""
    original_payload: dict
    failure_reason: str
    exception_type: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    retry_count: int = 0