#!/usr/bin/env python3
"""检查 CIO 公开 API 与核心代码的裸指针和异步引用约束。"""

from __future__ import annotations

import re
import sys
from pathlib import Path


SOURCE_SUFFIXES = {".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx", ".h"}


def strip_comments_and_literals(source: str) -> str:
    """保留换行位置，移除注释、字符串和字符字面量。"""
    result: list[str] = []
    index = 0
    state = "code"
    while index < len(source):
        current = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""

        if state == "code":
            if current == "/" and following == "/":
                result.extend((" ", " "))
                index += 2
                state = "line_comment"
                continue
            if current == "/" and following == "*":
                result.extend((" ", " "))
                index += 2
                state = "block_comment"
                continue
            if current == '"':
                result.append(" ")
                index += 1
                state = "string"
                continue
            if current == "'":
                result.append(" ")
                index += 1
                state = "character"
                continue
            result.append(current)
            index += 1
            continue

        if state == "line_comment":
            if current == "\n":
                result.append("\n")
                state = "code"
            else:
                result.append(" ")
            index += 1
            continue

        if state == "block_comment":
            if current == "*" and following == "/":
                result.extend((" ", " "))
                index += 2
                state = "code"
            else:
                result.append("\n" if current == "\n" else " ")
                index += 1
            continue

        if current == "\\":
            result.append(" ")
            if following:
                result.append("\n" if following == "\n" else " ")
            index += 2
            continue

        if (state == "string" and current == '"') or (
            state == "character" and current == "'"
        ):
            result.append(" ")
            index += 1
            state = "code"
            continue

        result.append("\n" if current == "\n" else " ")
        index += 1

    return "".join(result)


CHECKS = {
    "裸指针声明": re.compile(
        r"""
        (?:
          (?!return\b|co_return\b)
          \b(?:void|bool|char|wchar_t|char8_t|char16_t|char32_t|
              short|int|long|float|double|auto|[A-Za-z_]\w*(?:::\w+)*)
          (?:\s*<[^;{}()\n]*>)?
          (?:\s+const)?
        )
        \s*\*+\s*(?:const\s+)?[A-Za-z_]\w*
        """,
        re.VERBOSE,
    ),
    "显式 new 表达式": re.compile(r"\bnew\s+(?:[A-Za-z_:]|\()"),
    "显式 delete 表达式": re.compile(
        r"\bdelete(?:\s*\[\s*\])?\s+(?:[A-Za-z_]|\()"
    ),
    "reference_wrapper": re.compile(
        r"\bstd::(?:reference_wrapper|ref|cref)\s*(?:<|\()"
    ),
    "异步引用捕获": re.compile(r"\[\s*&|\[[^\]\n]*,\s*&"),
    "调度队列保存协程句柄": re.compile(
        r"\b(?:deque|queue)\s*<\s*(?:std::)?"
        r"(?:coroutine_handle|CoroutineRef)"
    ),
}

ALIASING_SHARED_PTR_FILES = {
    "include/cio/sync/mutex.hpp",
    "include/cio/sync/rwlock.hpp",
}

ALIASING_SHARED_PTR_EXPRESSION = re.compile(
    r"""
    std::shared_ptr\s*<[^;{}]+>\s*
    (?:[A-Za-z_]\w*\s*)?
    \{[^;{}]*std::addressof\s*\([^;{}]*\)[^;{}]*\}
    """,
    re.VERBOSE | re.DOTALL,
)


def line_number(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def main() -> int:
    if len(sys.argv) != 2:
        print("用法：check_core_contract.py <源码根目录>", file=sys.stderr)
        return 2

    root = Path(sys.argv[1]).resolve()
    files = sorted(
        path
        for folder in ("include", "src")
        for path in (root / folder).rglob("*")
        if path.is_file() and path.suffix in SOURCE_SUFFIXES
    )

    violations: list[str] = []
    for path in files:
        code = strip_comments_and_literals(path.read_text(encoding="utf-8"))
        relative = path.relative_to(root).as_posix()
        for label, pattern in CHECKS.items():
            for match in pattern.finditer(code):
                violations.append(
                    f"{relative}:{line_number(code, match.start())}: {label}"
                )

        # mapped guard 只允许把同步取得的子对象地址直接交给 shared_ptr
        # aliasing constructor。该表达式不声明、保存或公开裸指针，所有权仍由
        # shared_ptr 控制块表达；除此之外的 addressof 一律拒绝。
        alias_ranges = [
            (match.start(), match.end())
            for match in ALIASING_SHARED_PTR_EXPRESSION.finditer(code)
        ]
        for match in re.finditer(r"\bstd::addressof\s*\(", code):
            allowed = relative in ALIASING_SHARED_PTR_FILES and any(
                first <= match.start() < last for first, last in alias_ranges
            )
            if not allowed:
                violations.append(
                    f"{relative}:{line_number(code, match.start())}: "
                    "addressof 未直接包装为审核过的 aliasing shared_ptr"
                )

    if violations:
        print("CIO 核心源码契约检查失败：", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1

    print(f"CIO 核心源码契约检查通过：检查 {len(files)} 个文件")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
