"""Shared rate-limit instance.

Split into its own module (rather than living in main.py) so route files can
import `limiter` to tighten limits on specific expensive endpoints without a
circular import back to main.py.
"""
from slowapi import Limiter
from slowapi.util import get_remote_address

# Generous default for normal read endpoints — the UI polls a handful of
# routes per view, not per player. Expensive/scrapable endpoints (ETL
# triggers, external-fetch routes) set a tighter limit at the route level.
limiter = Limiter(key_func=get_remote_address, default_limits=["120/minute"])
