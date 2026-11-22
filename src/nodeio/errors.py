"""Errors raised while a graph is loaded or run."""


class GraphError(Exception):
    """The graph is not runnable.

    Attributes:
        cycle: Node ids on a cycle, when the failure is a cycle.
    """

    def __init__(self, message: str, cycle: tuple[str, ...] = ()) -> None:
        """Store the message and an optional cycle.

        Args:
            message: What failed.
            cycle: Node ids in order, including the repeated start.
        """
        super().__init__(message)
        self.cycle = cycle
