"""Git clean filter that removes outputs and execution counts from notebook code cells.

It reads a notebook on stdin and writes it to stdout in the Jupyter byte format. It writes input
that is not valid JSON without change. Do this setup one time in each clone:

    git config filter.nbstrip.clean "python3 deploy/nbstrip.py"
    git config filter.nbstrip.smudge cat
    git config filter.nbstrip.required true

The required filter also needs the smudge command, because git stops a checkout without one.
"""

import json
import sys


def strip(data):
    try:
        nb = json.loads(data.decode("utf-8"))
    except ValueError:
        return data
    if not isinstance(nb, dict):
        return data
    cells = nb.get("cells")
    if isinstance(cells, list):
        for cell in cells:
            if isinstance(cell, dict) and cell.get("cell_type") == "code":
                cell["outputs"] = []
                cell["execution_count"] = None
    text = json.dumps(nb, sort_keys=True, indent=1, ensure_ascii=False) + "\n"
    return text.encode("utf-8")


if __name__ == "__main__":
    sys.stdout.buffer.write(strip(sys.stdin.buffer.read()))
