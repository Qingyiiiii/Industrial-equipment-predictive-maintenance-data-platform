# -*- coding: utf-8 -*-
"""Run local code-quality checks that do not require the big-data cluster."""
import ast
import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECK_DIRS = ["src", "analysis", "streaming", "bin", "tests"]
EXCLUDE_NAMES = {"__pycache__"}
MARKDOWN_CHECK_FILES = [
    "README.md",
    "README_zh.md",
    "README_en.md",
    "项目接口文档.md",
    "通用大数据流程配置.md",
    "MetroPT-3虚拟机测试执行清单.md",
    "analysis/README_zh.md",
    "analysis/README_en.md",
    "streaming/README_zh.md",
    "streaming/README_en.md",
    "src/README_zh.md",
    "src/README_en.md",
    "bin/README_zh.md",
    "bin/README_en.md",
    "data/metropt_quality/README_zh.md",
    "data/metropt_quality/README_en.md",
    "data/metropt_quality/analysis/reports/README.md",
    "data/metropt_quality/analysis/reports/README_zh.md",
    "data/metropt_quality/analysis/reports/README_en.md",
    "data/metropt_quality/delivery_packages/README.md",
    "data/metropt_quality/delivery_packages/README_zh.md",
    "data/metropt_quality/delivery_packages/README_en.md",
    "api/README_zh.md",
    "api/README_en.md",
    "tests/README_zh.md",
    "tests/README_en.md",
]


def _iter_python_files():
    for dirname in CHECK_DIRS:
        base = ROOT / dirname
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if any(part in EXCLUDE_NAMES for part in path.parts):
                continue
            yield path


def check_ast() -> int:
    files = list(_iter_python_files())
    failures = []
    for path in files:
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            failures.append(f"{path}: {exc}")
    if failures:
        print("AST_CHECK=FAIL")
        for item in failures:
            print(item)
        return 1
    print(f"AST_CHECK=PASS files={len(files)}")
    return 0


def run_unittest() -> int:
    tests_dir = ROOT / "tests"
    if not tests_dir.exists():
        print("UNITTEST=SKIP reason=tests_dir_missing")
        return 0
    for path in [ROOT, ROOT / "src", ROOT / "streaming", ROOT / "analysis"]:
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    suite = unittest.defaultTestLoader.discover(str(tests_dir), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    status = "PASS" if result.wasSuccessful() else "FAIL"
    print(f"UNITTEST={status} tests={result.testsRun} failures={len(result.failures)} errors={len(result.errors)}")
    return 0 if result.wasSuccessful() else 1


def _is_external_or_nonlocal_link(target: str) -> bool:
    lowered = target.lower()
    return (
        not target
        or lowered.startswith(("http://", "https://", "mailto:", "#"))
        or target.startswith(("/", "\\"))
        or (len(target) >= 2 and target[1] == ":")
        or target.startswith("<")
    )


def check_markdown_links() -> int:
    pattern = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
    failures = []
    checked = 0
    for rel in MARKDOWN_CHECK_FILES:
        path = ROOT / rel
        if not path.exists():
            continue
        checked += 1
        text = path.read_text(encoding="utf-8")
        for match in pattern.finditer(text):
            target = match.group(1).strip()
            target = target.split("#", 1)[0].strip()
            if _is_external_or_nonlocal_link(target):
                continue
            candidate = (path.parent / target).resolve()
            if not candidate.exists():
                failures.append(f"{rel}: missing markdown link target: {target}")
    if failures:
        print("MARKDOWN_LINK_CHECK=FAIL")
        for item in failures:
            print(item)
        return 1
    print(f"MARKDOWN_LINK_CHECK=PASS files={checked}")
    return 0


def main() -> None:
    print(f"LOCAL_CODE_QUALITY root={ROOT}")
    rc = 0
    rc |= check_ast()
    rc |= run_unittest()
    rc |= check_markdown_links()
    if rc:
        print("LOCAL_CODE_QUALITY=FAIL")
        raise SystemExit(1)
    print("LOCAL_CODE_QUALITY=PASS")


if __name__ == "__main__":
    main()
