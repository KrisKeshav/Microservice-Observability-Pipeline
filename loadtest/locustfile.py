"""Locust load test — 50 users against a 3-connection pool guarantees pool exhaustion."""

import uuid
from locust import HttpUser, task, between


class OrderUser(HttpUser):
    wait_time = between(0.1, 0.3)

    @task(3)
    def create_order(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        self.client.post(
            "/api/orders",
            json={"order_id": order_id},
            headers={"X-Request-ID": f"lt-{uuid.uuid4().hex[:8]}"},
        )

    @task(7)
    def get_order(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        self.client.get(
            f"/api/orders/{order_id}",
            headers={"X-Request-ID": f"lt-{uuid.uuid4().hex[:8]}"},
        )
