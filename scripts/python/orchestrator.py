#!/usr/bin/env python3
"""CLI entry point for the dummy pipeline orchestrator."""
from __future__ import annotations

import argparse
import sys

from scripts.python.lib import registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dummy pipeline orchestrator")
    parser.add_argument("command", choices=["run", "clean", "graph", "list"], help="Command to execute")
    parser.add_argument("--only", dest="only", help="Optional target to limit execution")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dag = registry.build_registry()

    if args.command == "list":
        for task_name in sorted(dag.tasks):
            print(task_name)
        return 0

    if args.command == "graph":
        dag.print_graph()
        return 0

    if args.command == "clean":
        dag.clean_outputs()
        return 0

    if args.command == "run":
        dag.run(target=args.only)
        return 0

    raise ValueError(f"Unhandled command {args.command}")


if __name__ == "__main__":
    sys.exit(main())
