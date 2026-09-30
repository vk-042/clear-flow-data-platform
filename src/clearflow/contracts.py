"""Versioned contracts. Monetary amounts cross the wire as decimal strings."""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LIFECYCLES = {
    "banking": {None: {"initiated"}, "initiated": {"authorized", "declined"},
                "authorized": {"settled", "reversed"}, "settled": {"reversed"},
                "declined": set(), "reversed": set()},
    "healthcare": {None: {"submitted"}, "submitted": {"accepted", "denied"},
                   "accepted": {"paid", "denied"}, "paid": {"adjusted"},
                   "denied": set(), "adjusted": set()},
}

class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1]
    event_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,100}$")
    domain: Literal["banking", "healthcare"]
    entity_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,100}$")
    party_id: str = Field(pattern=r"^SYN-[a-zA-Z0-9_-]{1,80}$")
    source_system: Literal["synthetic-bank", "synthetic-claims"]
    sequence: int = Field(strict=True, ge=1, le=10000)
    status: str
    amount: Decimal = Field(gt=0, max_digits=14, decimal_places=2)
    currency: Literal["USD"]
    event_time: datetime

    @field_validator("amount", mode="before", json_schema_input_type=str)
    @classmethod
    def decimal_string(cls, value):
        if not isinstance(value, str):
            raise ValueError("amount must be a decimal string, never a JSON float")
        return value

    @field_validator("event_time")
    @classmethod
    def aware_time(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event_time requires a timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def domain_contract(self):
        expected = "synthetic-bank" if self.domain == "banking" else "synthetic-claims"
        if self.source_system != expected or self.status not in LIFECYCLES[self.domain]:
            raise ValueError("status/source_system does not match domain")
        return self

    def wire(self):
        value = self.model_dump(mode="json")
        value["amount"] = format(self.amount, ".2f")
        return value

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.wire(), sort_keys=True).encode()).hexdigest()
