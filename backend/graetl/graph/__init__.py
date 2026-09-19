"""Visual node graphs, compiled to plain Python.

A graph is a JSON document (:mod:`~graetl.graph.model`); every node's shape is
resolved from one of four sources (:mod:`~graetl.graph.registry`); the compiler
(:mod:`~graetl.graph.compiler`) walks the execution wire and writes an ordinary
module file beside the graph. Nothing interprets a graph at run time - by the
time a pipeline runs, there is only Python.
"""

from graetl.graph.build import (
    BuildReport,
    GraphBuild,
    build_pipeline_graphs,
    clean_generated,
    compile_one_graph,
    discover_graph_files,
    is_generated,
    output_for,
)
from graetl.graph.compiler import (
    CompileError,
    compile_function,
    compile_graphlib_file,
    compile_module_file,
    graph_digest,
)
from graetl.graph.model import (
    ANY,
    EXEC,
    Comment,
    Graph,
    GraphError,
    Link,
    Node,
    Pin,
    Variable,
)
from graetl.graph.registry import NodeDef, NodeRegistry, registry_for

__all__ = [
    "ANY",
    "EXEC",
    "BuildReport",
    "Comment",
    "CompileError",
    "Graph",
    "GraphBuild",
    "GraphError",
    "Link",
    "Node",
    "NodeDef",
    "NodeRegistry",
    "Pin",
    "Variable",
    "build_pipeline_graphs",
    "clean_generated",
    "compile_function",
    "compile_graphlib_file",
    "compile_module_file",
    "compile_one_graph",
    "discover_graph_files",
    "graph_digest",
    "is_generated",
    "output_for",
    "registry_for",
]
