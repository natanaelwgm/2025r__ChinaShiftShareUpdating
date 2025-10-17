"""Task registry and DAG execution utilities."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


@dataclass
class Task:
    name: str
    inputs: List[str]
    outputs: List[str]
    runner: Callable[[], None]


@dataclass
class DAG:
    tasks: Dict[str, Task] = field(default_factory=dict)
    adjacency: Dict[str, Set[str]] = field(default_factory=dict)

    def add_task(self, task: Task) -> None:
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name} already exists")
        self.tasks[task.name] = task
        for dependency in task.inputs:
            self.adjacency.setdefault(dependency, set()).add(task.name)

    def run(self, target: Optional[str] = None) -> None:
        order = list(self.tasks.values()) if target is None else [self.tasks[target]]
        for task in order:
            task.runner()

    def print_graph(self) -> None:
        for task_name, task in self.tasks.items():
            dependencies = ", ".join(task.inputs) or "(no deps)"
            print(f"{task_name} <- {dependencies}")

    def clean_outputs(self) -> None:
        raise NotImplementedError("Cleaning logic to be implemented")


def build_registry() -> DAG:
    dag = DAG()
    # Placeholder: register tasks once modules are implemented.
    return dag
