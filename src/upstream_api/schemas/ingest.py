from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ValidateStreamRequest(BaseModel):
    streamid: str = Field(min_length=10, max_length=4096)
    remote_addr: str | None = Field(default=None, max_length=256)


class ValidateStreamResponse(BaseModel):
    accept: bool
    session_id: UUID | None = None
    expected: dict[str, Any] | None = None
    reason: str | None = None
    reason_code: str | None = None


class ObservedStream(BaseModel):
    codec: str = Field(min_length=1, max_length=32)
    width: int = Field(ge=1, le=7680)
    height: int = Field(ge=1, le=4320)
    fps: float = Field(ge=0, le=1000)
    bitrate_kbps: int = Field(ge=0, le=200_000)


class StreamStartedRequest(BaseModel):
    session_id: UUID
    observed: ObservedStream
    started_at: datetime


class StreamEndedRequest(BaseModel):
    session_id: UUID
    ended_at: datetime
    # Optional: streaming-server hooks omit this and let the API derive
    # duration from the session's started_at/ended_at in the same transaction.
    duration_seconds: float | None = Field(default=None, ge=0)
    bytes_sent: int | None = Field(default=None, ge=0)
    reason: str = Field(min_length=1, max_length=64)


class ValidationFailedRequest(BaseModel):
    session_id: UUID
    observed: ObservedStream | None = None
    mismatch_fields: list[str] = Field(default_factory=list, max_length=16)
    failure_code: str = Field(min_length=1, max_length=64)
    failure_message: str = Field(min_length=1, max_length=512)


class IngestActionResponse(BaseModel):
    session_id: UUID
    state: str


class LookupSessionRequest(BaseModel):
    streamid: str = Field(min_length=10, max_length=4096)


class LookupSessionResponse(BaseModel):
    session_id: UUID
    account_id: UUID
    agent_id: UUID
    camera_id: UUID
    state: str


class MediaMTXAuthRequest(BaseModel):
    """MediaMTX 1.9.3 authHTTP request body.

    Shape is dictated by MediaMTX (see ``mediamtx.yml.default`` lines 87-96);
    every field is defaulted because MediaMTX may omit values, and a 422 on
    missing keys would falsely accept the publish (MediaMTX treats non-2xx
    as deny but we still want our logs to record the action+protocol it sent).
    Extra fields are ignored so a MediaMTX bump that adds keys does not break
    the contract.
    """

    user: str = ""
    password: str = ""
    ip: str = ""
    action: str = ""
    path: str = ""
    protocol: str = ""
    id: str = ""
    query: str = ""

    model_config = ConfigDict(extra="ignore")
