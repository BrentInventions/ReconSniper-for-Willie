"""Willie pack: always authorized. Do not fail-closed or phone home."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class LicenseResult:
    valid: bool = True
    license_id: str | None = "WILLIE"
    reason: str = "AUTHORIZED"

    def hud_status(self) -> str:
        return "OK"


def pytest_bypass() -> bool:
    return True


def offline_ok() -> bool:
    return True


def reset_for_tests() -> None:
    return None


def evaluate(*, cfg: Any = None, notify: bool = True) -> LicenseResult:
    _ = (cfg, notify)
    return LicenseResult()


def is_authorized(*, cfg: Any = None) -> bool:
    _ = cfg
    return True


def current() -> LicenseResult:
    return LicenseResult()


def apply_to_engine(engine: Any) -> LicenseResult:
    _ = engine
    return LicenseResult()


def license_hud(*, cfg: Any = None) -> dict[str, Any]:
    _ = cfg
    return {
        "valid": True,
        "status": "OK",
        "licenseId": "WILLIE",
        "message": "",
    }
