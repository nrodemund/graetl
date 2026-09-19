"""Browsing an importable module, so reflection is something you can look at.

Typing ``py:pandas.read_csv`` works, but only if you already know the name. The
editor's Import dialog asks this module instead: *what is in pandas?* - and gets
back its callables with their signatures and one-line docs, grouped so the
useful ones are at the top.

Three rules keep the list short enough to read:

* only public names, and only the ones the module itself defines or re-exports
  through ``__all__`` (so ``pandas.np`` and a hundred re-imported helpers do not
  drown ``read_csv``);
* functions and classes, because those are what a node can call;
* submodules are listed separately, so ``pandas.io`` is something you step into
  rather than something you try to call.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from typing import Any

from graetl.graph.model import GraphError
from graetl.graph.registry import annotation_name

#: Never worth offering: they are neither data work nor safe to call by accident.
_SKIP = {"main", "test", "tests", "setup", "exit", "quit", "help", "copyright", "credits"}


def _signature_text(target: Any) -> str:
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        return "(…)"
    parts: list[str] = []
    for name, parameter in signature.parameters.items():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            parts.append(f"*{name}")
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            parts.append(f"**{name}")
        elif parameter.default is not inspect.Parameter.empty:
            parts.append(f"{name}=…")
        else:
            parts.append(name)
    return f"({', '.join(parts)})"


def _first_line(target: Any) -> str:
    doc = (inspect.getdoc(target) or "").strip()
    if not doc:
        return ""
    for line in doc.splitlines():
        line = line.strip()
        if line:
            return line[:160]
    return ""


def _returns(target: Any) -> str:
    try:
        return annotation_name(inspect.signature(target).return_annotation)
    except (TypeError, ValueError):
        return "Any"


def import_module(name: str, *, allow: tuple[str, ...] = ()) -> Any:
    """Import a module by name, with the configured allowlist applied."""
    name = (name or "").strip()
    if not name:
        raise GraphError("a module name is required")
    root = name.split(".", 1)[0]
    if allow and root not in allow:
        raise GraphError(
            f"importing {name!r} is not allowed - [graphs] reflect_allow lists "
            f"{', '.join(allow)}"
        )
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        raise GraphError(f"no module named {name!r} - is it installed?") from None
    except Exception as exc:  # noqa: BLE001 - an import can do anything
        raise GraphError(f"importing {name!r} failed: {type(exc).__name__}: {exc}") from None


def browse(name: str, *, allow: tuple[str, ...] = (), limit: int = 400) -> dict[str, Any]:
    """What is inside a module: its callables, its classes and its submodules."""
    module = import_module(name, allow=allow)
    exported = getattr(module, "__all__", None)
    names = list(exported) if isinstance(exported, (list, tuple)) else dir(module)

    functions: list[dict[str, Any]] = []
    classes: list[dict[str, Any]] = []
    for attribute in sorted(set(names)):
        if attribute.startswith("_") or attribute.lower() in _SKIP:
            continue
        try:
            target = getattr(module, attribute)
        except Exception:  # noqa: BLE001 - a property on a module can raise
            continue
        if inspect.ismodule(target):
            continue
        if not callable(target):
            continue
        # Without __all__, keep only what this module actually defines -
        # otherwise every import it made shows up as one of its functions.
        if exported is None:
            origin = getattr(target, "__module__", "") or ""
            if origin and not (origin == name or origin.startswith(name + ".")):
                continue
        entry = {
            "name": attribute,
            "op": f"py:{name}.{attribute}",
            "signature": _signature_text(target),
            "doc": _first_line(target),
            "returns": _returns(target),
            "kind": "class" if inspect.isclass(target) else "function",
        }
        (classes if entry["kind"] == "class" else functions).append(entry)

    return {
        "module": name,
        "doc": _first_line(module),
        "file": getattr(module, "__file__", None),
        "functions": functions[:limit],
        "classes": classes[:limit],
        "submodules": submodules(module)[:limit],
    }


def submodules(module: Any) -> list[str]:
    """Importable children of a package, so the dialog can step into it."""
    path = getattr(module, "__path__", None)
    if not path:
        return []
    out: list[str] = []
    try:
        for info in pkgutil.iter_modules(path):
            if not info.name.startswith("_"):
                out.append(f"{module.__name__}.{info.name}")
    except Exception:  # noqa: BLE001 - a broken package must not break the dialog
        return []
    return sorted(out)


def members_of(dotted: str, *, allow: tuple[str, ...] = (), limit: int = 200) -> dict[str, Any]:
    """The methods of a *type*, for calling something on a value.

    ``df.to_csv(...)`` is a method on a value, not a module-level function, so
    reflection alone cannot reach it. This lists what a type can do, and the
    editor turns a pick into a ``core:method`` node.
    """
    module_name, _, attribute = dotted.rpartition(".")
    if not module_name:
        raise GraphError(f"{dotted!r} is not a dotted path to a type")
    module = import_module(module_name, allow=allow)
    target = getattr(module, attribute, None)
    if target is None:
        raise GraphError(f"{module_name} has no attribute {attribute!r}")

    out: list[dict[str, Any]] = []
    for attribute_name in sorted(set(dir(target))):
        if attribute_name.startswith("_"):
            continue
        try:
            member = inspect.getattr_static(target, attribute_name)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(member, (staticmethod, classmethod)):
            member = member.__func__
        if not callable(member):
            continue
        out.append(
            {
                "name": attribute_name,
                "op": "core:method",
                "config": {"name": attribute_name},
                "signature": _signature_text(member),
                "doc": _first_line(member),
                "kind": "method",
            }
        )
    return {"type": dotted, "methods": out[:limit]}
