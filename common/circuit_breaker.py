import asyncio
import enum
import time
from typing import Any, Callable, Coroutine

from prometheus_client import Counter, Gauge

from common.logging import get_logger, log_event

logger = get_logger("circuit-breaker")


class CircuitState(enum.IntEnum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class CircuitBreakerOpenException(Exception):
    def __init__(self, service: str, target: str, retry_after: float):
        super().__init__(f"Circuit breaker for {target} is OPEN. Try again after {retry_after:.1f}s.")
        self.service = service
        self.target = target
        self.retry_after = retry_after


CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "State of circuit breaker: 0=CLOSED, 1=HALF_OPEN, 2=OPEN",
    ["service", "target"],
)

CIRCUIT_BREAKER_REJECTIONS = Counter(
    "circuit_breaker_rejections_total",
    "Total requests rejected by circuit breaker",
    ["service", "target"],
)

CIRCUIT_BREAKER_TRIPS = Counter(
    "circuit_breaker_trips_total",
    "Total times circuit breaker transitioned to OPEN",
    ["service", "target"],
)


class CircuitBreaker:
    def __init__(
        self,
        service: str,
        target: str,
        failure_threshold: int = 3,
        recovery_timeout: float = 5.0,
        half_open_success_threshold: int = 1,
    ):
        self.service = service
        self.target = target
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = time.time()
        self._lock = asyncio.Lock()

        CIRCUIT_BREAKER_STATE.labels(service=self.service, target=self.target).set(self.state)

    def _update_state(self, new_state: CircuitState, request_id: str | None = None) -> None:
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()
        CIRCUIT_BREAKER_STATE.labels(service=self.service, target=self.target).set(self.state)

        if new_state == CircuitState.OPEN and old_state != CircuitState.OPEN:
            CIRCUIT_BREAKER_TRIPS.labels(service=self.service, target=self.target).inc()
            log_event(
                logger,
                "circuit_tripped",
                request_id or "system",
                service=self.service,
                target=self.target,
                state="OPEN",
            )
        elif new_state == CircuitState.CLOSED and old_state != CircuitState.CLOSED:
            log_event(
                logger,
                "circuit_closed",
                request_id or "system",
                service=self.service,
                target=self.target,
                state="CLOSED",
            )
        elif new_state == CircuitState.HALF_OPEN and old_state != CircuitState.HALF_OPEN:
            log_event(
                logger,
                "circuit_half_open",
                request_id or "system",
                service=self.service,
                target=self.target,
                state="HALF_OPEN",
            )

    async def call(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        request_id: str | None = None,
        retries: int = 1,
        backoff_seconds: float = 0.05,
        **kwargs: Any,
    ) -> Any:
        async with self._lock:
            now = time.time()
            if self.state == CircuitState.OPEN:
                if now - self.last_state_change >= self.recovery_timeout:
                    self._update_state(CircuitState.HALF_OPEN, request_id)
                    self.success_count = 0
                else:
                    CIRCUIT_BREAKER_REJECTIONS.labels(service=self.service, target=self.target).inc()
                    retry_after = max(0.1, self.recovery_timeout - (now - self.last_state_change))
                    raise CircuitBreakerOpenException(self.service, self.target, retry_after)

        last_exception = None
        for attempt in range(retries + 1):
            try:
                result = await func(*args, **kwargs)
                await self._on_success(request_id)
                return result
            except Exception as exc:
                last_exception = exc
                if attempt < retries:
                    await asyncio.sleep(backoff_seconds * (2**attempt))

        await self._on_failure(request_id)
        raise last_exception

    async def _on_success(self, request_id: str | None) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.half_open_success_threshold:
                    self.failure_count = 0
                    self._update_state(CircuitState.CLOSED, request_id)
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    async def _on_failure(self, request_id: str | None) -> None:
        async with self._lock:
            self.failure_count += 1
            if self.state == CircuitState.HALF_OPEN or self.failure_count >= self.failure_threshold:
                self._update_state(CircuitState.OPEN, request_id)
