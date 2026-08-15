import os

os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["DATABASE_URL"] = "postgresql://orders:orders@localhost:5432/orders"
