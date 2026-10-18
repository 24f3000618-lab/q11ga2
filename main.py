import base64
import math
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------
# YOUR ASSIGNED VALUES
# ---------------------------------------------------------------
TOTAL_ORDERS = 48          # T: the fixed catalog has IDs 1..48
RATE_LIMIT = 15            # R: max requests per client...
WINDOW_SECONDS = 10        # ...per 10 seconds

app = FastAPI(title="Orders API")

# ---------------------------------------------------------------
# 1) IDEMPOTENT POST /orders
# ---------------------------------------------------------------
# Remembers: Idempotency-Key  ->  the order we created for it
idempotency_store: dict[str, dict] = {}
store_lock = threading.Lock()


@app.post("/orders", status_code=201)
def create_order(
    body: Optional[dict] = Body(default=None),
    idempotency_key: Optional[str] = Header(default=None),
):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    # The lock makes sure two simultaneous requests with the same key
    # can't both think "I'm first" and create two orders.
    with store_lock:
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]  # same order, no duplicate

        order = {
            "id": str(uuid.uuid4()),
            "status": "created",
            "details": body or {},
            "created_at": time.time(),
        }
        idempotency_store[idempotency_key] = order
        return order


# ---------------------------------------------------------------
# 2) CURSOR PAGINATION  GET /orders?limit=P&cursor=C
# ---------------------------------------------------------------
def encode_cursor(position: int) -> str:
    """Turn a number into an opaque-looking string."""
    return base64.urlsafe_b64encode(f"pos:{position}".encode()).decode()


def decode_cursor(cursor: str) -> int:
    try:
        text = base64.urlsafe_b64decode(cursor.encode()).decode()
        if not text.startswith("pos:"):
            raise ValueError
        position = int(text[4:])
        if position < 0:
            raise ValueError
        return position
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=100),
    cursor: Optional[str] = Query(default=None),
):
    start = decode_cursor(cursor) if cursor else 0   # how many we've already served
    end = min(start + limit, TOTAL_ORDERS)

    items = [
        {"id": i, "status": "created", "name": f"Order {i}"}
        for i in range(start + 1, end + 1)           # ids start at 1
    ]
    next_cursor = encode_cursor(end) if end < TOTAL_ORDERS else None

    return {"items": items, "next_cursor": next_cursor}


# ---------------------------------------------------------------
# 3) PER-CLIENT RATE LIMITING (sliding window)
# ---------------------------------------------------------------
# Remembers: client id -> timestamps of its recent requests
request_log: dict[str, deque] = defaultdict(deque)
rate_lock = threading.Lock()


@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    client_id = request.headers.get("X-Client-Id")

    # Let CORS preflight checks through, and skip callers with no client id.
    if request.method == "OPTIONS" or not client_id:
        return await call_next(request)

    now = time.monotonic()
    with rate_lock:
        timestamps = request_log[client_id]

        # Forget requests older than the window
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(1, math.ceil(WINDOW_SECONDS - (now - timestamps[0])))
            return JSONResponse(
                status_code=429,
                content={"detail": "Too Many Requests"},
                headers={"Retry-After": str(retry_after)},
            )

        timestamps.append(now)

    return await call_next(request)


# ---------------------------------------------------------------
# CORS  (added LAST so it wraps everything, including 429 responses)
# ---------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)


@app.get("/")
def root():
    return {"status": "ok", "total_orders": TOTAL_ORDERS}
