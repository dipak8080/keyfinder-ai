"""Credit-based paywall for the GPU tools.

Self-contained: nothing outside this package is imported, so it can be dropped
into the existing backend without touching config.py, jobs.py or main.py yet.

Ships inert — with PAYWALL_ENABLED unset, nothing is charged and nothing is
blocked.
"""

__all__ = ["config", "db", "security"]