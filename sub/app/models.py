from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MessageIn(BaseModel):
    """
    Model for new message coming into the database.
    Used when we want to save a new message.
    """

    data: str  # The message text content
    category: str  # What category this message belongs to
    created_at: datetime  # When this message was created


class MessageOut(BaseModel):
    """
    Model for message coming out of the database.
    Used when we read a message that was already saved.
    """

    id: Optional[str] = Field(default=None, alias="_id")  # Database ID
    data: str  # The message text content
    category: str  # What category this message belongs to
    created_at: datetime  # When this message was created

    model_config = {"populate_by_name": True}
