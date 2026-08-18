import asyncio
from unittest.mock import AsyncMock

import pytest

from common.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException, CircuitState


def test_circuit_breaker_closed_success():
    async def _test():
        cb = CircuitBreaker("test-service", "target-service", failure_threshold=2, recovery_timeout=1.0)
        mock_func = AsyncMock(return_value="ok")

        result = await cb.call(mock_func)
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    asyncio.run(_test())


def test_circuit_breaker_trips_to_open():
    async def _test():
        cb = CircuitBreaker("test-service", "target-service", failure_threshold=2, recovery_timeout=0.2)
        mock_func = AsyncMock(side_effect=RuntimeError("connection error"))

        # Attempt 1 -> fails
        with pytest.raises(RuntimeError):
            await cb.call(mock_func, retries=0)
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 1

        # Attempt 2 -> fails and reaches threshold (2) -> trips to OPEN
        with pytest.raises(RuntimeError):
            await cb.call(mock_func, retries=0)
        assert cb.state == CircuitState.OPEN

        # Immediate next attempt -> fail fast without calling func
        mock_func.reset_mock()
        with pytest.raises(CircuitBreakerOpenException) as exc_info:
            await cb.call(mock_func)
        assert exc_info.value.target == "target-service"
        assert mock_func.call_count == 0

    asyncio.run(_test())


def test_circuit_breaker_recovery_half_open_to_closed():
    async def _test():
        cb = CircuitBreaker("test-service", "target-service", failure_threshold=1, recovery_timeout=0.1)
        failing_func = AsyncMock(side_effect=RuntimeError("downstream down"))

        # Trip to OPEN
        with pytest.raises(RuntimeError):
            await cb.call(failing_func, retries=0)
        assert cb.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Next call should transition to HALF_OPEN and succeed -> resets to CLOSED
        success_func = AsyncMock(return_value="recovered")
        result = await cb.call(success_func, retries=0)
        assert result == "recovered"
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    asyncio.run(_test())


def test_circuit_breaker_half_open_failure_re_trips():
    async def _test():
        cb = CircuitBreaker("test-service", "target-service", failure_threshold=1, recovery_timeout=0.1)
        failing_func = AsyncMock(side_effect=RuntimeError("downstream down"))

        # Trip to OPEN
        with pytest.raises(RuntimeError):
            await cb.call(failing_func, retries=0)
        assert cb.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Next call transitions to HALF_OPEN but fails -> immediately re-trips to OPEN
        with pytest.raises(RuntimeError):
            await cb.call(failing_func, retries=0)
        assert cb.state == CircuitState.OPEN

    asyncio.run(_test())
