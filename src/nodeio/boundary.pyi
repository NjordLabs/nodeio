import logging

from nodeio.graph import GraphDefinition

def otel_tracer() -> object | None: ...
def named_logger(name: str) -> logging.Logger: ...
async def http_exchange(
    method: str,
    url: str,
    timeout: float,
    verify: bool | str,
    cert: tuple[str, str] | None,
    max_bytes: int,
    host: str = "",
) -> tuple[int, bytes]: ...
async def _read_limited(response: object, max_bytes: int) -> bytes: ...
def grpc_channel(
    target: str,
    allow_insecure: bool,
    root_certificates: bytes | None,
    private_key: bytes | None,
    certificate_chain: bytes | None,
) -> object: ...

class Observer:
    def on_start(self, definition: GraphDefinition, node_id: str, *, run_id: str) -> None: ...
    def on_success(
        self,
        definition: GraphDefinition,
        node_id: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None: ...
    def on_exception(
        self,
        definition: GraphDefinition,
        node_id: str,
        exception: str,
        *,
        run_id: str,
        duration_s: float,
    ) -> None: ...
