# Section 3 Problem 2 — Food Ordering System (gRPC)

A central server holds a fixed set of restaurants and the state of every order.
Customer clients place and track orders; restaurant clients receive them and
advance their status. Built with gRPC, Python 3.

Section 3 of the assignment offers two problems — Collaborative Document Editing
and this one. **This is Problem 2.**

Full command reference, including the cluster: **[run.md](run.md)**.

---

## 1. Files

| File | What it is |
| ---- | ---------- |
| [food_ordering.proto](../food_ordering.proto) | the service definition and its messages |
| [server.py](../server.py) | restaurants, orders, state validation, streaming, locking |
| [customer.py](../customer.py) | the customer CLI |
| [restaurant.py](../restaurant.py) | the restaurant CLI |

`food_ordering_pb2.py` and `food_ordering_pb2_grpc.py` are generated from the
`.proto` and are not in git — generated gRPC code refuses to load unless it
matches the installed grpcio version.

---

## 2. Setup

```bash
pip install grpcio grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto
```

Re-run the second command after any change to the `.proto`, or clients fail with
`Method not found`.

On RCE the system `python3` is 3.6 and too old for grpcio — use
`~/HW3/venv/bin/python3` in place of `python3` everywhere below.

---

## 3. The API

| RPC | Kind | Purpose |
| --- | ---- | ------- |
| `ListRestaurants` | unary | the restaurants and their menus |
| `PlaceOrder` | unary | place an order, returns a unique id |
| `GetOrderStatus` | unary | the current status of an order |
| `UpdateOrderStatus` | unary | a restaurant advances an order |
| `SubscribeToOrderUpdates` | **server-streaming** | a customer is pushed every status change |

### Order states

```
PLACED ──► ACCEPTED ──► PREPARING ──► READY
   │
   └─────► CANCELLED
```

Anything not on that diagram is rejected. An order can be cancelled only while
still `PLACED`; once a restaurant has accepted it, cancelling is refused.

---

## 4. Running the demonstration

Three terminals. The server address is a command-line argument, as the
assignment specifies.

**Terminal A — server**

```bash
python3 server.py localhost:50051
```

**Terminal B — customer**

```bash
python3 customer.py localhost:50051
```

```
1. List Restaurants   2. Place Order   3. Check Order Status
4. Track Order        5. Cancel Order  6. Exit
```

**Terminal C — restaurant** (its name is the second argument, so it sees only
its own orders)

```bash
python3 restaurant.py localhost:50051 "Pizza House"
```

```
1. View Pending Orders   2. Accept Order   3. Start Preparing
4. Mark Ready            5. Exit
```

### The six required demonstrations

**1. Listing restaurants** — Terminal B, option `1`:

```
Pizza House
  Margherita Pizza - 250
  Farmhouse Pizza  - 350
  Garlic Bread     - 150
Burger Point
  Veg Burger    - 180
  Cheese Burger - 220
  French Fries  - 120
```

**2. Placing an order** — Terminal B, option `2`:

```
Restaurant name: Pizza House
Food item (blank to finish): Margherita Pizza
Quantity: 2
Food item (blank to finish):
→ Order O1 placed, total 500, status PLACED
```

**3. A restaurant processing the order** — Terminal C, option `1` shows the new
order; then `2` (Accept), `3` (Start Preparing), `4` (Mark Ready), entering `O1`
each time.

**4. A customer receiving real-time updates** — before step 3, Terminal B,
option `4`, order id `O1`. That terminal then prints each change **as the
restaurant makes it**, without asking:

```
[update] O1 -> ACCEPTED
[update] O1 -> PREPARING
[update] O1 -> READY
```

This is `SubscribeToOrderUpdates`, the server-streaming RPC. Nothing polls.

**5. Two clients interacting concurrently** — open a fourth terminal with a
second customer and place an order while the first is still tracking. Both are
served at once, and the order ids differ.

**6. Exception cases with gRPC status codes**

| Do this | Result |
| ------- | ------ |
| Place an order at `Nonexistent Cafe` | `NOT_FOUND` — restaurant not found |
| Order `Sushi` from Pizza House | `NOT_FOUND` — item not available |
| Check status of order `ZZZ` | `NOT_FOUND` — order not found |
| From Burger Point's terminal, update a Pizza House order | `PERMISSION_DENIED` |
| Mark an order `READY` while still `PLACED` | `FAILED_PRECONDITION` |
| Cancel an order already `ACCEPTED` | `FAILED_PRECONDITION` |

The three codes carry different meanings: `NOT_FOUND` says the thing does not
exist, `PERMISSION_DENIED` says it exists but is not yours, and
`FAILED_PRECONDITION` says it exists and is yours but the system is in the wrong
state. A client can act differently on each.

### On the cluster

Same thing across machines — the server binds `0.0.0.0` so other nodes can reach
it, and clients are given `<server-node>:<port>`. Full steps, including choosing
a port that will not collide with other students, are in
**[run.md](run.md#3-on-the-rce-cluster)**.

---

## 5. Design notes

**Concurrency.** All shared state — the order table and the subscriber lists —
is protected by a single lock. The critical sections are a dictionary lookup and
an assignment, so one lock costs nothing; per-order locks would add a way to
deadlock in exchange for contention that does not exist at this scale.

**Notifications go out outside the lock.** Each subscriber has its own queue.
The handler copies the subscriber list while holding the lock, releases it, and
only then posts the update. Notifying inside the lock would let one slow
subscriber block every other request.

**The transition table lives in one place.** Keeping the permitted transitions
in a single table, rather than as `if` statements spread through the handlers,
is what makes it possible to say with confidence that a `READY` order can never
go back to `PREPARING`.

**A thread-pool limit, found by measurement.** `SubscribeToOrderUpdates`
originally waited on its queue with no timeout while holding one thread from a
pool of ten, so ten tracking customers consumed every thread and the server
stopped answering anything — `ListRestaurants` returned `DEADLINE_EXCEEDED`.
Every individual operation was correct; it failed only when enough clients did a
legitimate thing at once. The queue is now waited on with a one-second timeout
inside a loop that re-checks whether the client is still connected, and the pool
is 64 workers. Forty simultaneous trackers now leave the server responding
immediately.

**`ListRestaurantOrders`** is an extra RPC beyond the specified five, added so a
restaurant client can list its own pending orders without tracking state itself.

**Cancellation** is expressed through `UpdateOrderStatus` with status
`CANCELLED`, permitted only from `PLACED`.
