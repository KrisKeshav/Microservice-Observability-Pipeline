import asyncio
from unittest.mock import AsyncMock

import pytest

from common.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException, CircuitState


def _breaker(threshold: int = 2, recovery: float = 0.1) -> CircuitBreaker:
    return CircuitBreaker("test-service", "target-service", failure_threshold=threshold, recovery_timeout=recovery)


def test_closed_success():
    async def _test():
        cb = _breaker()
        assert await cb.call(AsyncMock(return_value="ok")) == "ok"
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    asyncio.run(_test())


def test_trips_to_open_and_fails_fast():
    async def _test():
        cb = _breaker(threshold=2, recovery=60)
        failing = AsyncMock(side_effect=RuntimeError("down"))

        with pytest.raises(RuntimeError):
            await cb.call(failing)
        assert cb.state == CircuitState.CLOSED

        with pytest.raises(RuntimeError):
            await cb.call(failing)
        assert cb.state == CircuitState.OPEN

        failing.reset_mock()
        with pytest.raises(CircuitBreakerOpenException) as exc_info:
            await cb.call(failing)
        assert failing.call_count == 0
        assert exc_info.value.retry_after_header == "60"

    asyncio.run(_test())


def test_half_open_success_closes():
    async def _test():
        cb = _breaker(threshold=1)
        with pytest.raises(RuntimeError):
            await cb.call(AsyncMock(side_effect=RuntimeError("down")))
        await asyncio.sleep(0.15)

        assert await cb.call(AsyncMock(return_value="recovered")) == "recovered"
        assert cb.state == CircuitState.CLOSED

    asyncio.run(_test())


def test_half_open_failure_reopens():
    async def _test():
        cb = _breaker(threshold=1)
        failing = AsyncMock(side_effect=RuntimeError("down"))
        with pytest.raises(RuntimeError):
            await cb.call(failing)
        await asyncio.sleep(0.15)

        with pytest.raises(RuntimeError):
            await cb.call(failing)
        assert cb.state == CircuitState.OPEN

    asyncio.run(_test())


def test_half_open_allows_only_one_probe():
    async def _test():
        cb = _breaker(threshold=1)
        with pytest.raises(RuntimeError):
            await cb.call(AsyncMock(side_effect=RuntimeError("down")))
        await asyncio.sleep(0.15)

        release = asyncio.Event()

        async def slow_probe():
            await release.wait()
            return "ok"

        probe = asyncio.create_task(cb.call(slow_probe))
        await asyncio.sleep(0)
        assert cb.state == CircuitState.HALF_OPEN

        with pytest.raises(CircuitBreakerOpenException):
            await cb.call(AsyncMock(return_value="second"))

        release.set()
        assert await probe == "ok"
        assert cb.state == CircuitState.CLOSED

    asyncio.run(_test())


def test_retries_only_listed_exceptions():
    async def _test():
        cb = _breaker(threshold=5)
        flaky = AsyncMock(side_effect=[ConnectionError("refused"), "ok"])
        assert await cb.call(flaky, retries=1, retry_on=(ConnectionError,), backoff_seconds=0) == "ok"
        assert flaky.call_count == 2

        not_retried = AsyncMock(side_effect=TimeoutError("slow"))
        with pytest.raises(TimeoutError):
            await cb.call(not_retried, retries=1, retry_on=(ConnectionError,), backoff_seconds=0)
        assert not_retried.call_count == 1

    asyncio.run(_test())
