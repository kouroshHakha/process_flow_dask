from typing import Callable
import dill
import pickle

class Node:
    def __init__(self, func: Callable = None, name: str = None):
        self.task = func
        self.code_string = dill.dumps(func)

    def __str__(self):
        return f'{str(self.task)}'

    def __call__(self, *args, **kwargs):
        return self.task(*args, **kwargs)

class ProcessGraph:

    def __init__(self, graph: nx.DiGraph):
        self._graph = graph


    def find_entry_point(self):
        result = []
        for node in self._graph:
            if self._graph.in_degree[node] == 0:
                result.append(node)
        return result

    def is_node_rdy(self, node):
        return True

    def run(self, input: List[Any]):
        queue = self.find_entry_point()

        while queue:
            node = queue.pop()
            if self.is_node_rdy(node):
                run_node.delay(node.code_string)
                queue = list(self._graph.successors(node)) + queue

