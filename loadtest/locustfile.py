import uuid
from locust import HttpUser, task, between


class OrderUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(5)
    def get_order(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        req_id = f"loadtest-get-{uuid.uuid4().hex[:8]}"
        self.client.get(
            f"/api/orders/{order_id}",
            headers={"X-Request-ID": req_id},
        )

    @task(3)
    def create_order(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        req_id = f"loadtest-post-{uuid.uuid4().hex[:8]}"
        self.client.post(
            "/api/orders",
            json={"order_id": order_id},
            headers={"X-Request-ID": req_id},
        )

    @task(1)
    def trigger_forced_error(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        req_id = f"loadtest-err-{uuid.uuid4().hex[:8]}"
        self.client.get(
            f"/api/orders/{order_id}",
            headers={
                "X-Request-ID": req_id,
                "X-Demo-Scenario": "error",
            },
        )

    @task(1)
    def trigger_slow_request(self):
        order_id = f"load-{uuid.uuid4().hex[:8]}"
        req_id = f"loadtest-slow-{uuid.uuid4().hex[:8]}"
        self.client.get(
            f"/api/orders/{order_id}",
            headers={
                "X-Request-ID": req_id,
                "X-Demo-Scenario": "slow",
            },
        )
