"""Cell dependency graph: deterministic topo order, cycle detection, downstream sets.

``edges`` maps each cell to its upstream dependencies. Ties in the topological
order break by the cells' position in ``nodes`` (file order).
"""

from __future__ import annotations

from typing import Callable

from .errors import CycleError


def topo_sort(nodes: list[str], edges: dict[str, list[str]]) -> list[str]:
    done: set[str] = set()
    order: list[str] = []
    while len(order) < len(nodes):
        progressed = False
        for n in nodes:
            if n not in done and all(u in done for u in edges.get(n, ())):
                order.append(n)
                done.add(n)
                progressed = True
        if not progressed:
            raise CycleError(f"dependency cycle: {_find_cycle(nodes, edges, done)}")
    return order


def _find_cycle(nodes: list[str], edges: dict[str, list[str]], done: set[str]) -> str:
    state: dict[str, bool] = {}
    path: list[str] = []

    def dfs(n: str) -> str | None:
        state[n] = True
        path.append(n)
        for u in edges.get(n, ()):
            if u in done:
                continue
            if state.get(u):
                return " -> ".join(path[path.index(u) :] + [u])
            if u not in state:
                found = dfs(u)
                if found:
                    return found
        path.pop()
        state[n] = False
        return None

    for n in nodes:
        if n not in done and n not in state:
            found = dfs(n)
            if found:
                return found
    return " -> ".join(n for n in nodes if n not in done)


def downstream(
    nodes: list[str],
    edges: dict[str, list[str]],
    changed: set[str],
    through: Callable[[str], bool] = lambda n: True,
) -> list[str]:
    """The changed cells plus everything reachable from them, in topo order.
    Only nodes passing ``through`` are reached, and only they reach further."""
    rev: dict[str, list[str]] = {n: [] for n in nodes}
    for n in nodes:
        for u in edges.get(n, ()):
            rev.setdefault(u, []).append(n)
    hit: set[str] = set()
    stack = [n for n in nodes if n in changed]
    while stack:
        n = stack.pop()
        if n not in hit and through(n):
            hit.add(n)
            stack.extend(rev.get(n, ()))
    return [n for n in topo_sort(nodes, edges) if n in hit]
