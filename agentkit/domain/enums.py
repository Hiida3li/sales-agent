"""Enumerations used across the domain."""

from enum import Enum


class ExecutionStatus(str, Enum):
    """Lifecycle of a single function execution within an interaction.

    Inherits from ``str`` so the members serialize directly to the same string
    literals used on the wire (``"queued"``, ``"pending"``, ``"completed"``).
    """

    QUEUED = "queued"
    PENDING = "pending"
    COMPLETED = "completed"
