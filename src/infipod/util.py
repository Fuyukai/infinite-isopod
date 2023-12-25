import collections
import re
from collections.abc import Awaitable, Callable, Iterable
from functools import partial

import anyio

TO_SNAKE_CASE = re.compile(r"(?<!^)(?=[A-Z])")


# https://stackoverflow.com/a/44969381
def pascal_to_snake(word: str) -> str:
    """
    Converts a string from PascalCase to snake_case.
    """

    return "".join(["_" + c.lower() if c.isupper() else c for c in word]).lstrip("_")


async def map_as_completed[Input, Result](
    inputs: Iterable[Input], fn: Callable[[Input], Awaitable[Result]]
) -> Iterable[Result]:
    """
    Applies the provided ``fn`` to the iterable of inputs and returns them in the as-completed
    order.
    """

    # this would, ideally, be an async generator, but because async generators can be dropped
    # without deterministic cleanup, that corrupts nurseries. so yay.

    results = collections.deque[Result]()

    async def run(fn: Callable[[], Awaitable[Result]]) -> None:
        results.appendleft(await fn())

    async with anyio.create_task_group() as group:
        for thing in inputs:
            invoke_fn = partial(run, partial(fn, thing))
            group.start_soon(invoke_fn)

    return results
