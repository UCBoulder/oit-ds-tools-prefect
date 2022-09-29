"""UCB implementation of ConcurrentTaskRunner"""

import asyncio
from contextlib import AsyncExitStack
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Set,
)
from uuid import UUID

import anyio
from anyio import CapacityLimiter

from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType

class LimitedTaskRunner(BaseTaskRunner):
    """
    ConcurrentTaskRunner but with limited threads
    """

    def __init__(self, max_workers: int =None):
        # Runtime attributes
        self._task_group: anyio.abc.TaskGroup = None
        self._result_events: Dict[UUID, anyio.abc.Event] = {}
        self._results: Dict[UUID, Any] = {}
        self._keys: Set[UUID] = set()
        self._max_workers = max_workers
        self._limiter = None

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.CONCURRENT

    async def submit(
        self,
        key: UUID,
        call: Callable[[], Awaitable[State[R]]],
    ) -> None:
        if not self._started:
            raise RuntimeError(
                "The task runner must be started before submitting work."
            )

        if not self._task_group:
            raise RuntimeError(
                "The concurrent task runner cannot be used to submit work after "
                "serialization."
            )

        self._result_events[key] = anyio.Event()

        # Rely on the event loop for concurrency
        self._task_group.start_soon(self._run_and_store_result, key, call)

        # Track the keys so we can ensure to gather them later
        self._keys.add(key)

    async def wait(
        self,
        key: UUID,
        timeout: float = None,
    ) -> Optional[State]:
        if not self._task_group:
            raise RuntimeError(
                "The concurrent task runner cannot be used to wait for work after "
                "serialization."
            )

        return await self._get_run_result(key, timeout)

    async def _run_and_store_result(
        self, key: UUID, call: Callable[[], Awaitable[State[R]]]
    ):
        """
        Simple utility to store the orchestration result in memory on completion

        Since this run is occuring on the main thread, we capture exceptions to prevent
        task crashes from crashing the flow run.
        """
        try:
            if self._limiter is not None:
                async with self._limiter:
                    self._results[key] = await call()
            else:
                self._results[key] = await call()
        except BaseException as exc:
            self._results[key] = exception_to_crashed_state(exc)

        self._result_events[key].set()

    async def _get_run_result(
        self, key: UUID, timeout: float = None
    ) -> Optional[State]:
        """
        Block until the run result has been populated.
        """
        result = None  # Return value on timeout

        with anyio.move_on_after(timeout):

            # Attempt to use the event to wait for the result. This is much more efficient
            # than the spin-lock that follows but does not work if the wait call
            # happens from an event loop in a different thread than the one from which
            # the event was created
            result_event = self._result_events[key]
            if result_event._event._loop == asyncio.get_running_loop():
                await result_event.wait()

            result = self._results.get(key)
            while not result:
                await anyio.sleep(0)  # yield to other tasks
                result = self._results.get(key)

        return result

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the process pool
        """
        if self._max_workers is not None:
            self._limiter = CapacityLimiter(self._max_workers)
        self._task_group = await exit_stack.enter_async_context(
            anyio.create_task_group()
        )

    def __getstate__(self):
        """
        Allow the `ConcurrentTaskRunner` to be serialized by dropping the task group.
        """
        data = self.__dict__.copy()
        data.update({k: None for k in {"_task_group"}})
        return data

    def __setstate__(self, data: dict):
        """
        When deserialized, we will no longer have a reference to the task group.
        """
        self.__dict__.update(data)
        self._task_group = None
