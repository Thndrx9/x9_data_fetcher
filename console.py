"""
Shared terminal color scheme for the whole system.

Every module's log lines get colored by the same rule, based on the tag
already present in the message text — nothing needs to pass an explicit
color. Auto-disables when stdout isn't a real terminal (piped to a file,
CI, etc.) or when NO_COLOR is set, so redirected/logged output stays plain.

Scheme:
    [ERROR]                        -> red
    [WARN]                         -> yellow
    [PRUNE]                        -> magenta   (destructive action, stand out)
    "connected" / "complete" / "done" -> green
    anything else                  -> unchanged
"""

import os
import sys

COLOR_ENABLED = sys.stdout.isatty() and not os.getenv("NO_COLOR")

_RED = "\033[31m"
_YELLOW = "\033[33m"
_MAGENTA = "\033[35m"
_GREEN = "\033[32m"
_RESET = "\033[0m"


def colorize(text: str) -> str:
    if not COLOR_ENABLED:
        return text
    if "[ERROR]" in text:
        color = _RED
    elif "[WARN]" in text:
        color = _YELLOW
    elif "[PRUNE]" in text:
        color = _MAGENTA
    elif "connected" in text or "complete" in text or "done" in text:
        color = _GREEN
    else:
        return text
    return f"{color}{text}{_RESET}"
