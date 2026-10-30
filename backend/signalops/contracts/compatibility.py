"""Structural backward-compatibility check between two JSON Schema versions.

Within a contract version only additive optional changes are allowed. This check
reports the structural breaks a consumer of the old schema would hit: removed
properties, newly required properties, changed types, and removed enumeration
or constant values. Tightened bounds (lengths, ranges, patterns) are not
detected and remain a review responsibility.
"""

from typing import Any

type Schema = dict[str, Any]


def _resolve(node: Schema, root: Schema) -> Schema:
    seen: set[str] = set()
    while "$ref" in node:
        ref = node["$ref"]
        if ref in seen or not ref.startswith("#/"):
            break
        seen.add(ref)
        target: Any = root
        for part in ref[2:].split("/"):
            target = target[part]
        node = target
    return node


def _values(node: Schema) -> set[Any] | None:
    if "enum" in node:
        return {repr(value) for value in node["enum"]}
    if "const" in node:
        return {repr(node["const"])}
    return None


def _variants(node: Schema) -> list[Schema] | None:
    for key in ("anyOf", "oneOf"):
        if key in node:
            variants: list[Schema] = node[key]
            return variants
    return None


def _signature(node: Schema, root: Schema) -> str:
    # Matches alternatives across versions; enumeration values are compared afterwards.
    node = _resolve(node, root)
    if "title" in node or "type" in node:
        return str(node.get("title") or node["type"])
    return "values" if _values(node) is not None else "any"


def breaking_changes(old: Schema, new: Schema) -> list[str]:
    """Return human-readable breaks from `old` to `new`; empty when compatible."""
    found: list[str] = []
    _compare(old, new, old, new, "$", found)
    return found


def _compare(
    old: Schema, new: Schema, old_root: Schema, new_root: Schema, path: str, found: list[str]
) -> None:
    old = _resolve(old, old_root)
    new = _resolve(new, new_root)

    old_variants, new_variants = _variants(old), _variants(new)
    if old_variants is not None or new_variants is not None:
        _compare_variants(old, new, old_root, new_root, path, found)
        return

    if "type" in old and old.get("type") != new.get("type"):
        found.append(f"{path}: type changed from {old.get('type')} to {new.get('type')}")
        return

    old_values, new_values = _values(old), _values(new)
    if old_values is not None:
        removed = old_values - (new_values if new_values is not None else old_values)
        if removed:
            found.append(f"{path}: values removed: {', '.join(sorted(removed))}")

    old_properties: Schema = old.get("properties", {})
    new_properties: Schema = new.get("properties", {})
    for name, old_property in old_properties.items():
        if name not in new_properties:
            found.append(f"{path}.{name}: property removed")
            continue
        _compare(old_property, new_properties[name], old_root, new_root, f"{path}.{name}", found)
    newly_required = set(new.get("required", [])) - set(old.get("required", []))
    for name in sorted(newly_required):
        found.append(f"{path}.{name}: property became required")

    if isinstance(old.get("items"), dict) and isinstance(new.get("items"), dict):
        _compare(old["items"], new["items"], old_root, new_root, f"{path}[]", found)
    if isinstance(old.get("additionalProperties"), dict) and isinstance(
        new.get("additionalProperties"), dict
    ):
        _compare(
            old["additionalProperties"],
            new["additionalProperties"],
            old_root,
            new_root,
            f"{path}{{}}",
            found,
        )


def _compare_variants(
    old: Schema, new: Schema, old_root: Schema, new_root: Schema, path: str, found: list[str]
) -> None:
    old_variants = _variants(old) or [old]
    new_variants = _variants(new) or [new]
    new_by_signature = {_signature(variant, new_root): variant for variant in new_variants}
    for variant in old_variants:
        signature = _signature(variant, old_root)
        if signature not in new_by_signature:
            found.append(f"{path}: alternative {signature} removed")
            continue
        _compare(variant, new_by_signature[signature], old_root, new_root, path, found)
