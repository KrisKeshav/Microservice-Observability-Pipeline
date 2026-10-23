"""Async circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

State checks and transitions never await, so they are atomic on the event loop
and no lock is needed.
"""

import asyncio
import enum
import math
import time
from typing import Any, Awaitable, Callable

from prometheus_client import Counter, Gauge

from common.logging import get_logger, log_event, log_warning

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

    @property
    def retry_after_header(self) -> str:
        return str(max(1, math.ceil(self.retry_after)))


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
    def __init__(self, service: str, target: str, failure_threshold: int = 3, recovery_timeout: float = 5.0):
        self.service = service
        self.target = target
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.reset()

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.opened_at = 0.0
        self._probe_in_flight = False
        CIRCUIT_BREAKER_STATE.labels(service=self.service, target=self.target).set(self.state)

    def _set_state(self, new_state: CircuitState, request_id: str | None) -> None:
        if new_state == self.state:
            return
        self.state = new_state
        CIRCUIT_BREAKER_STATE.labels(service=self.service, target=self.target).set(new_state)
        if new_state == CircuitState.OPEN:
            self.opened_at = time.monotonic()
            CIRCUIT_BREAKER_TRIPS.labels(service=self.service, target=self.target).inc()
            log_warning(logger, "circuit_opened", request_id or "system", service=self.service, target=self.target)
        else:
            log_event(logger, f"circuit_{new_state.name.lower()}", request_id or "system",
                      service=self.service, target=self.target)

    def _reject(self, retry_after: float) -> None:
        CIRCUIT_BREAKER_REJECTIONS.labels(service=self.service, target=self.target).inc()
        raise CircuitBreakerOpenException(self.service, self.target, retry_after)

    def _before_call(self, request_id: str | None) -> None:
        if self.state == CircuitState.OPEN:
            elapsed = time.monotonic() - self.opened_at
            if elapsed < self.recovery_timeout:
                self._reject(self.recovery_timeout - elapsed)
            self._set_state(CircuitState.HALF_OPEN, request_id)

        if self.state == CircuitState.HALF_OPEN:
            # Only one probe request is allowed through while we test recovery.
            if self._probe_in_flight:
                self._reject(1.0)
            self._probe_in_flight = True

    def _on_success(self, request_id: str | None) -> None:
        self._probe_in_flight = False
        self.failure_count = 0
        self._set_state(CircuitState.CLOSED, request_id)

    def _on_failure(self, request_id: str | None) -> None:
        self._probe_in_flight = False
        self.failure_count += 1
        if self.state == CircuitState.HALF_OPEN or self.failure_count >= self.failure_threshold:
            self._set_state(CircuitState.OPEN, request_id)

    async def call(
        self,
        func: Callable[..., Awaitable[Any]],
        *args: Any,
        request_id: str | None = None,
        retries: int = 0,
        retry_on: tuple[type[BaseException], ...] = (),
        backoff_seconds: float = 0.05,
        **kwargs: Any,
    ) -> Any:
        """Run func through the breaker.

        Only exceptions listed in retry_on are retried. Retrying timeouts or 5xx
        responses would add load to a dependency that is already struggling.
        """
        self._before_call(request_id)
        try:
            for attempt in range(retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    break
                except retry_on:
                    if attempt == retries:
                        raise
                    await asyncio.sleep(backoff_seconds * (2**attempt))
        except Exception:
            self._on_failure(request_id)
            raise
        self._on_success(request_id)
        return result
