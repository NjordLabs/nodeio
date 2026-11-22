# nodeio

Async graph engine for Python 3.13 services. A graph is validated once.
Each call runs on its own values.

The package is not on PyPI. Install the release tag:

```bash
pip install "nodeio @ git+https://github.com/NjordLabs/nodeio.git@v0.2.0"
```

HTTP and gRPC clients are extras: `nodeio[http]` and `nodeio[grpc]`.
Add them to the same install, for example
`nodeio[http] @ git+https://github.com/NjordLabs/nodeio.git@v0.2.0`.
A timeout is required. Plain HTTP and insecure gRPC stay off unless
the caller opts in.

The engine uses [pymjolnir](https://github.com/NjordLabs/pymjolnir)
for its factory and [mimirlog](https://github.com/NjordLabs/mimirlog)
for the logging observer. The observer records the run id, the node,
and the duration. It does not record payloads.

```python
import asyncio

from nodeio import Edge, Node, Port, load, run


async def double(inputs, side):
    del side
    return {"value": int(inputs["value"]) * 2}


source = Node(
    "in", "map", (Port("value", "int"),), (Port("value", "int"),), double
)
sink = Node(
    "out", "map", (Port("value", "int"),), (Port("value", "int"),), double
)
graph = load(
    (source, sink),
    (Edge("in", "value", "out", "value"),),
    entries=(("in", "value"),),
)
print(asyncio.run(run(graph, {"in": {"value": 2}}))["out"]["value"])
```

`gate`, `mux`, `demux`, and `subgraph_node` are routing nodes.
`load(...).to_dot()` and `to_mermaid()` export the graph.

Source and issues: [NjordLabs/nodeio](https://github.com/NjordLabs/nodeio)
