from contextvars import ContextVar
from uuid import UUID

request_id_var: ContextVar[str] = ContextVar("request_id", default="")
account_id_var: ContextVar[UUID | None] = ContextVar("account_id", default=None)
agent_id_var: ContextVar[UUID | None] = ContextVar("agent_id", default=None)
session_id_var: ContextVar[UUID | None] = ContextVar("session_id", default=None)
