from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    _id: Optional[str] = Field(default=None, alias="_id")
    data: str
    category: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {
        "populate_by_name": True,
    }