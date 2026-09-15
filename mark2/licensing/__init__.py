"""Willie Recon Sniper is authorized on this pack. No phone-home license wall."""

from .manager import LicenseResult, evaluate, is_authorized, license_hud, reset_for_tests

__all__ = [
    "LicenseResult",
    "evaluate",
    "is_authorized",
    "license_hud",
    "reset_for_tests",
]
