"""
SSE Event Schema untuk Wikipedia RecentChange events.
Digunakan untuk validasi event yang diterima dari Wikimedia EventStreams.
"""

from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, Dict, Any
from datetime import datetime


class EventMeta(BaseModel):
    """Metadata dari event Wikimedia"""
    id: str
    dt: datetime
    topic: str
    partition: int
    offset: int


class EventData(BaseModel):
    """Data dari recentchange event"""
    schema_field: str = Field(alias="$schema")
    id: str
    type: str
    namespace: int
    title: str
    comment: str
    timestamp: int
    user: str
    bot: bool
    server_url: str
    server_name: str
    server_script_path: str
    wiki: str
    parsedcomment: str
    minor: bool = False  # absent for non-edit types (log, categorize, external)
    length: Dict[str, int] = Field(default_factory=dict)
    revision: Dict[str, int] = Field(default_factory=dict)
    namespacename: Optional[str] = None
    pageurl: Optional[str] = None
    diffurl: Optional[str] = None
    log_type: Optional[str] = None
    log_action: Optional[str] = None
    log_params: Optional[Any] = None  # can be a dict or a list depending on log_action


class WikimediaEvent(BaseModel):
    """Event lengkap dari Wikimedia EventStreams"""
    type: str
    topic: str
    source: str
    specversion: str
    id: str
    time: datetime
    data: EventData
    meta: EventMeta


class ValidationError(Exception):
    """Custom exception untuk validation errors"""
    pass