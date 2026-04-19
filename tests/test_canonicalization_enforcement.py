from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
CANONICAL_FILE = SRC / "canonicalization.py"


def test_no_hashlib_or_uuid5_outside_canonicalization_module():
    violations = []
    for py_file in SRC.rglob("*.py"):
        if py_file == CANONICAL_FILE:
            continue
        text = py_file.read_text(encoding="utf-8")
        if "import hashlib" in text or "from hashlib" in text:
            violations.append(f"{py_file}: hashlib import")
        if "uuid.uuid5(" in text or "uuid5(" in text:
            violations.append(f"{py_file}: uuid5 usage")
    assert not violations, "Forbidden hashing primitives outside canonicalization.py:\n" + "\n".join(violations)
