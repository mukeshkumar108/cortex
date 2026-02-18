from typing import Any, Dict, Iterable, List, Optional


def extract_node_dicts(row: Any, required_keys: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    """
    Extract candidate node dicts from a Falkor/Graphiti row.

    Row shapes are inconsistent across driver versions:
    - dict of column -> value
    - list/tuple
    - dict whose values are node dicts (one per column)
    - list/tuple of node dicts
    - header rows ("name", "summary", "uuid")
    """
    required = set(required_keys or [])
    nodes: List[Dict[str, Any]] = []

    def _normalize(d: Dict[str, Any]) -> Dict[str, Any]:
        # Some drivers wrap node properties under "properties"
        if isinstance(d.get("properties"), dict):
            return d["properties"]
        return d

    def _is_header(d: Dict[str, Any]) -> bool:
        # Common header markers
        if d.get("name") == "name" or d.get("summary") == "summary" or d.get("uuid") == "uuid":
            return True
        return False

    def _accept(d: Dict[str, Any]) -> bool:
        if not isinstance(d, dict):
            return False
        d = _normalize(d)
        if _is_header(d):
            return False
        if required and not required.issubset(set(d.keys())):
            return False
        return True

    if isinstance(row, dict):
        # If row is already a node dict
        if _accept(row):
            nodes.append(_normalize(row))
        # If row values are node dicts
        for value in row.values():
            if isinstance(value, dict) and _accept(value):
                nodes.append(_normalize(value))
    elif isinstance(row, (list, tuple)):
        for value in row:
            if isinstance(value, dict) and _accept(value):
                nodes.append(_normalize(value))

    return nodes


def pick_first_node(rows: Iterable[Any], required_keys: Optional[Iterable[str]] = None) -> Optional[Dict[str, Any]]:
    for row in rows:
        nodes = extract_node_dicts(row, required_keys=required_keys)
        if nodes:
            return nodes[0]
    return None


def extract_count(rows: Iterable[Any]) -> Optional[int]:
    """Extract integer count from Falkor results."""
    for row in rows:
        if isinstance(row, dict):
            value = row.get("count")
            if isinstance(value, dict) and "count" in value:
                value = value.get("count")
            if isinstance(value, (int, float)):
                return int(value)
        elif isinstance(row, (list, tuple)) and row:
            value = row[0]
            if isinstance(value, (int, float)):
                return int(value)
    return None
