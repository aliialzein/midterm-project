"""Environment-backed configuration for the metadata service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Runtime configuration for the metadata API and GC worker.

    The service deliberately reads all operational settings from environment
    variables so the same process can run in Docker or locally without a code
    change. These values are also used by the GC loop and placement logic.
    """

    database_url: str = ""
    replication_factor: int = 3
    vnodes: int = 128
    heartbeat_timeout: float = 15.0
    gc_interval: float = 10.0
    gc_grace_seconds: float = 60.0
    disable_gc: bool = False
    metadata_url: str = ""

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/dsfs"),
            replication_factor=int(os.getenv("REPLICATION_FACTOR", "3")),
            vnodes=int(os.getenv("VNODES", "128")),
            heartbeat_timeout=float(os.getenv("HEARTBEAT_TIMEOUT", "15")),
            gc_interval=float(os.getenv("GC_INTERVAL", "10")),
            gc_grace_seconds=float(os.getenv("GC_GRACE_SECONDS", "60")),
            disable_gc=os.getenv("DISABLE_GC", "0") not in {"", "0", "false", "False"},
            metadata_url=os.getenv("METADATA_URL", ""),
        )
