"""tvparser package exports (lazy loads submodules)."""

__all__ = [
    "cli",
    "core",
    "io",
    "csv2json",
    "slicer",
    "timeutils",
    "extract_slrun",
]

import importlib
from typing import Any


def __getattr__(name: str) -> Any:
    """
    Lazy-load submodule on attribute access, e.g.
    `from tvparser import extract_slrun` or `tvparser.extract_slrun`.
    """
    if name in __all__:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attr {name!r}")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
