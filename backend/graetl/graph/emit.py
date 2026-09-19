"""Small helpers for writing Python that a person will read.

Two jobs:

``Emitter``  indentation, blank-line hygiene, comment wrapping.
``Expr``     an expression plus its precedence, so the compiler can decide when
             a bracket is actually needed. ``a + b * c`` should come out as
             ``a + b * c``, not ``(a + (b * c))`` - the parentheses a naive
             generator adds are the main reason generated code reads like
             generated code.
``Names``    unique, legal, readable identifiers.
"""

from __future__ import annotations

import builtins
import keyword
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

INDENT = "    "

# Higher binds tighter. Matches Python's grammar closely enough for codegen.
P_LAMBDA = 1
P_TERNARY = 2
P_OR = 3
P_AND = 4
P_NOT = 5
P_COMPARE = 6
P_BIT_OR = 7
P_BIT_XOR = 8
P_BIT_AND = 9
P_SHIFT = 10
P_ADD = 11
P_MUL = 12
P_UNARY = 13
P_POW = 14
P_CALL = 15
P_ATOM = 16

BINARY_PRECEDENCE: dict[str, int] = {
    "or": P_OR, "and": P_AND,
    "==": P_COMPARE, "!=": P_COMPARE, "<": P_COMPARE, "<=": P_COMPARE,
    ">": P_COMPARE, ">=": P_COMPARE, "in": P_COMPARE, "not in": P_COMPARE,
    "is": P_COMPARE, "is not": P_COMPARE,
    "|": P_BIT_OR, "^": P_BIT_XOR, "&": P_BIT_AND,
    "+": P_ADD, "-": P_ADD,
    "*": P_MUL, "/": P_MUL, "//": P_MUL, "%": P_MUL,
    "**": P_POW,
}


@dataclass(frozen=True, slots=True)
class Expr:
    """A Python expression and how tightly it binds."""

    text: str
    precedence: int = P_ATOM

    def wrapped(self, minimum: int) -> str:
        """The text, bracketed only if it would otherwise re-associate."""
        return f"({self.text})" if self.precedence < minimum else self.text

    def __str__(self) -> str:  # pragma: no cover - convenience
        return self.text


def atom(text: str) -> Expr:
    return Expr(text, P_ATOM)


def call(text: str) -> Expr:
    return Expr(text, P_CALL)


def binary(symbol: str, left: Expr, right: Expr) -> Expr:
    """``left <symbol> right`` with the minimum number of brackets.

    Left-associative operators need brackets on the right at equal precedence
    (``a - (b - c)``); ``**`` is right-associative, so it is the other way round.
    """
    precedence = BINARY_PRECEDENCE[symbol]
    if symbol == "**":
        left_text = left.wrapped(precedence + 1)
        right_text = right.wrapped(precedence)
    else:
        left_text = left.wrapped(precedence)
        right_text = right.wrapped(precedence + 1)
    return Expr(f"{left_text} {symbol} {right_text}", precedence)


def unary(symbol: str, operand: Expr) -> Expr:
    precedence = P_NOT if symbol.strip() == "not" else P_UNARY
    space = " " if symbol.strip() == "not" else ""
    return Expr(f"{symbol.strip()}{space}{operand.wrapped(precedence)}", precedence)


def ternary(condition: Expr, if_true: Expr, if_false: Expr) -> Expr:
    return Expr(
        f"{if_true.wrapped(P_OR)} if {condition.wrapped(P_OR)} else {if_false.wrapped(P_TERNARY)}",
        P_TERNARY,
    )


def literal(value: Any) -> Expr:
    """A Python literal for a JSON-ish value."""
    if value is None:
        return atom("None")
    if isinstance(value, bool):
        return atom("True" if value else "False")
    if isinstance(value, (int, float)):
        return Expr(repr(value), P_ATOM if value >= 0 else P_UNARY)
    if isinstance(value, str):
        return atom(_quote(value))
    if isinstance(value, (list, tuple)):
        inner = ", ".join(literal(v).text for v in value)
        return atom(f"[{inner}]" if isinstance(value, list) else f"({inner},)" if len(value) == 1 else f"({inner})")
    if isinstance(value, dict):
        inner = ", ".join(f"{literal(k).text}: {literal(v).text}" for k, v in value.items())
        return atom("{" + inner + "}")
    return atom(repr(value))


def _quote(text: str) -> str:
    """Prefer double quotes, as black would."""
    if '"' not in text:
        return '"' + text.replace("\\", "\\\\").replace("\n", "\\n") + '"'
    if "'" not in text:
        return "'" + text.replace("\\", "\\\\").replace("\n", "\\n") + "'"
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def call_lines(prefix: str, arguments: list[str], *, width: int = 96) -> list[str]:
    """``prefix(a, b, c)`` on one line, or one argument per line when it is long."""
    single = f"{prefix}({', '.join(arguments)})"
    if len(single) <= width or not arguments:
        return [single]
    out = [f"{prefix}("]
    out.extend(f"{INDENT}{argument}," for argument in arguments)
    out.append(")")
    return out


class Names:
    """Hands out unique, legal, readable Python identifiers."""

    def __init__(self, reserved: set[str] | None = None) -> None:
        self.used: set[str] = set(reserved or set())

    def reserve(self, name: str) -> str:
        self.used.add(name)
        return name

    def make(self, desired: str, fallback: str = "value") -> str:
        base = _identifier(desired) or fallback
        # Never shadow a builtin: a local called ``round`` would break the next
        # call to round().
        if keyword.iskeyword(base) or keyword.issoftkeyword(base) or hasattr(builtins, base):
            base = f"{base}_"
        if base not in self.used:
            self.used.add(base)
            return base
        for index in range(2, 1000):
            candidate = f"{base}_{index}"
            if candidate not in self.used:
                self.used.add(candidate)
                return candidate
        raise RuntimeError(f"cannot allocate a name for {desired!r}")  # pragma: no cover


def _identifier(value: str) -> str:
    out: list[str] = []
    for char in str(value).strip().lower():
        if char.isalnum() or char == "_":
            out.append(char)
        elif out and out[-1] != "_":
            out.append("_")
    text = "".join(out).strip("_")
    while "__" in text:
        text = text.replace("__", "_")
    if text and text[0].isdigit():
        text = f"_{text}"
    return text


class Emitter:
    """Collects lines with indentation and sane blank-line behaviour."""

    def __init__(self, indent: int = 0) -> None:
        self.lines: list[str] = []
        self.level = indent

    def line(self, text: str = "") -> None:
        self.lines.append(f"{INDENT * self.level}{text}" if text else "")

    def blank(self, count: int = 1) -> None:
        """Ensure ``count`` blank lines here. Never leads a file with blanks."""
        if not self.lines:
            return
        have = 0
        for line in reversed(self.lines):
            if line != "":
                break
            have += 1
        for _ in range(max(0, count - have)):
            self.lines.append("")

    def comment(self, text: str, width: int = 88) -> None:
        available = max(20, width - len(INDENT) * self.level - 2)
        for paragraph in str(text).splitlines():
            if not paragraph.strip():
                self.line("#")
                continue
            for wrapped in textwrap.wrap(paragraph.strip(), available) or [""]:
                self.line(f"# {wrapped}")

    def docstring(self, text: str, width: int = 88) -> None:
        body = str(text).strip()
        if not body:
            return
        available = max(20, width - len(INDENT) * self.level - 6)
        paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]
        wrapped: list[str] = []
        for paragraph in paragraphs:
            wrapped.extend(textwrap.wrap(paragraph, available) or [""])
            wrapped.append("")
        wrapped = wrapped[:-1] if wrapped else wrapped
        if len(wrapped) == 1 and len(wrapped[0]) + 6 <= width:
            self.line(f'"""{wrapped[0]}"""')
            return
        self.line('"""' + (wrapped[0] if wrapped else ""))
        for extra in wrapped[1:]:
            self.line(extra)
        self.line('"""')

    @contextmanager
    def block(self) -> Iterator[None]:
        self.level += 1
        try:
            yield
        finally:
            self.level -= 1

    def extend(self, other: Emitter) -> None:
        self.lines.extend(other.lines)

    @property
    def empty(self) -> bool:
        return not any(line.strip() for line in self.lines)

    def render(self) -> str:
        while self.lines and self.lines[-1] == "":
            self.lines.pop()
        return "\n".join(self.lines) + "\n"
