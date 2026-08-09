CREATE TABLE IF NOT EXISTS orders (
    id         TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id          SERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_sec  INT         NOT NULL,
    error_count INT         NOT NULL,
    total_count INT         NOT NULL,
    error_rate  REAL        NOT NULL,
    details     JSONB
);
