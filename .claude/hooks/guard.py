#!/usr/bin/env python3
"""PreToolUse hook. Refuses agent access to .env and agent writes into raw/ or vendor/."""
import json
import os
import re
import sys

WRITE_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
PROTECTED = ("data/raw", "data/vendor")
ENV_IN_SHELL = re.compile(r"""(^|[\s'"=/<>|;&(])\.env(?![\w.])""")


def deny(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(2)


def main() -> None:
    try:
        event = json.load(sys.stdin)
    except ValueError:
        return
    tool = event.get("tool_name", "")
    args = event.get("tool_input") or {}
    root = os.environ.get("CLAUDE_PROJECT_DIR") or event.get("cwd") or os.getcwd()

    if tool == "Bash":
        if ENV_IN_SHELL.search(args.get("command", "")):
            deny("Do not name .env in a shell command. Secrets come only from config.settings.")
        return

    path = args.get("file_path") or args.get("notebook_path") or ""
    if not path:
        return
    real = os.path.realpath(os.path.join(root, path))
    if os.path.basename(real) == ".env":
        deny("Do not read or write .env. Secrets come only from config.settings.")
    if tool in WRITE_TOOLS:
        for sub in PROTECTED:
            base = os.path.realpath(os.path.join(root, sub))
            if real == base or real.startswith(base + os.sep):
                deny(f"Do not write under {sub}/. Use the Write-Audit-Publish flow.")


if __name__ == "__main__":
    main()
