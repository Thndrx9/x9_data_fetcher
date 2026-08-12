"""
Shared terminal color scheme for the whole system.

Every module's log lines get colored by the same rule, based on the tag
already present in the message text — nothing needs to pass an explicit
color. Auto-disables when stdout isn't a real terminal (piped to a file,
CI, etc.) or when NO_COLOR is set, so redirected/logged output stays plain.

Only the bracketed [TAG] portions of a line are colored — the message body
after them stays plain. Which color applies is still decided by the line
as a whole (e.g. a line is "an error line" if it contains [ERROR]
anywhere), but the color paint itself only lands on the [...] segments.

Scheme:
    [ERROR]     -> red      (failure)
    [WARN]      -> yellow
    [PRUNE]     -> magenta  (destructive action, stand out)
    [HEARTBEAT] -> blue
    everything else -> green (success/normal operation, by default)
"""

import os
import re
import sys

COLOR_ENABLED = sys.stdout.isatty() and not os.getenv("NO_COLOR")

_RED = "\033[31m"
_YELLOW = "\033[33m"
_MAGENTA = "\033[35m"
_GREEN = "\033[32m"
_BLUE = "\033[34m"
_RESET = "\033[0m"

_TAG_RE = re.compile(r"\[[^\[\]]*\]")


def colorize(text: str) -> str:
    if not COLOR_ENABLED:
        return text
    if "[ERROR]" in text:
        color = _RED
    elif "[WARN]" in text:
        color = _YELLOW
    elif "[PRUNE]" in text:
        color = _MAGENTA
    elif "[HEARTBEAT]" in text:
        color = _BLUE
    else:
        # Not an error/warning/prune/heartbeat line — treat as normal,
        # successful operation by default rather than requiring specific
        # success keywords (which misses things like "Authenticated",
        # "Subscribed", "connectable" that mean success but don't contain
        # the literal words "connected"/"complete"/"done").
        color = _GREEN
    # Color only the [TAG] segments; everything else in the line — the
    # message body — is left as plain text.
    return _TAG_RE.sub(lambda m: f"{color}{m.group(0)}{_RESET}", text)
