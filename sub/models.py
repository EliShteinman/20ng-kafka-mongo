from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class MessageIn(BaseModel):
    data: str
    category: str
    created_at: datetime

class MessageOut(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
    data: str
    category: str
    created_at: datetime

    model_config = {"populate_by_name": True}