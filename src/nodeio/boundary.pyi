import logging

from nodeio.graph import GraphDefinition

def named_logger(name: str) -> logging.Logger: ...
async def http_exchange(
    method: str,
    url: str,
    timeout: float,
    verify: bool | str,
    cert: tuple[str, str] | None,
) -> tuple[int, bytes]: ...

class Observer:
    def on_start(
        self, definition: GraphDefinition, node_id: str, *, run_id: str
    ) -> None: ...
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
