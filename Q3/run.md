# Section 3 Problem 2 — how to run it

A food ordering system: one server, any number of customer clients and
restaurant clients. Everything below is run from this folder, `Q3`.

---

## 1. Setup

```bash
pip install grpcio grpcio-tools
```

The two generated files, `food_ordering_pb2.py` and `food_ordering_pb2_grpc.py`,
are **not** in git, because generated gRPC code refuses to load unless it
matches the installed grpcio version. Generate them once:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto
```

> **Re-run that after any change to `food_ordering.proto`.** Otherwise the
> server uses stale stubs and clients fail with `Method not found`, which looks
> like a network problem but is not.

On RCE the system `python3` is 3.6 and too old for grpcio, so use the
virtualenv: `~/HW3/venv/bin/python3` in place of `python3` everywhere below.

---

## 2. On one machine

Three terminals.

**Terminal A — the server.** The address is a command-line argument, as the
assignment requires:

```bash
python3 server.py localhost:50051
```

**Terminal B — a customer:**

```bash
python3 customer.py localhost:50051
```

```
1. List Restaurants
2. Place Order
3. Check Order Status
4. Track Order
5. Cancel Order
6. Exit
```

**Terminal C — a restaurant.** The restaurant's name is the second argument, so
it only ever sees its own orders:

```bash
python3 restaurant.py localhost:50051 "Pizza House"
```

```
1. View Pending Orders
2. Accept Order
3. Start Preparing
4. Mark Ready
5. Exit
```

---

## 3. On the RCE cluster

### Three rules

1. **Never run servers on the login node.** Hold machines with `salloc` first.
2. **You can only `ssh` to a node you currently hold**, or you get
   `Access denied by pam_slurm_adopt`.
3. **Do not use port 50051.** It is the example port in the RCE guide, so every
   student tries to bind it. If someone else already has it, your server dies
   and your client silently connects to *theirs* — which shows up as
   `Method not found`, not as an error you would recognise. Pick something
   derived from your user id, e.g. `$((50000 + $(id -u) % 9000))`.

### Steps

```bash
ssh <your-username>@rce.iiit.ac.in
salloc --nodes=3 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
scontrol show hostnames $SLURM_JOB_NODELIST      # e.g. node01 node02 node03
PORT=$((50000 + $(id -u) % 9000))
echo $PORT
```

**Terminal A — the server, on the first node.** Bind `0.0.0.0` so the other
nodes can reach it; `localhost` would only accept connections from itself:

```bash
ssh node01
cd ~/HW3/Q3
~/HW3/venv/bin/python3 server.py 0.0.0.0:$PORT
```

**Terminal B — a customer, on the second node:**

```bash
ssh node02
cd ~/HW3/Q3
~/HW3/venv/bin/python3 customer.py node01:$PORT
```

**Terminal C — a restaurant, on the third node:**

```bash
ssh node03
cd ~/HW3/Q3
~/HW3/venv/bin/python3 restaurant.py node01:$PORT "Pizza House"
```

Open a fourth terminal with a second customer to show two clients interacting
with the server at the same time.

---

## 4. The demonstration

This is the sequence the assignment asks for, in order.

| # | Who | Does | Shows |
| - | --- | ---- | ----- |
| 1 | customer | `1. List Restaurants` | Pizza House and Burger Point with their menus |
| 2 | customer | `2. Place Order` — Pizza House, 2 × Margherita Pizza | a unique order id, total 500, status `PLACED` |
| 3 | customer | `4. Track Order` with that id | subscribes; the terminal now waits for updates |
| 4 | restaurant | `1. View Pending Orders` | the new order appears |
| 5 | restaurant | `2. Accept Order` | customer's tracking terminal prints `ACCEPTED` **on its own** |
| 6 | restaurant | `3. Start Preparing`, then `4. Mark Ready` | `PREPARING` then `READY` arrive the same way |
| 7 | second customer | places an order at the same time | a different order id; both are served concurrently |

Step 5 is the one worth pausing on: nobody asked for that update. The customer
subscribed once, and the server pushed the change over a server-streaming RPC.

### The exception cases

Each returns a gRPC status code rather than crashing:

| Try this | Result |
| -------- | ------ |
| Place an order at `"Nonexistent Cafe"` | `NOT_FOUND` — restaurant not found |
| Order `"Sushi"` from Pizza House | `NOT_FOUND` — item not available |
| Check status of order `ZZZ` | `NOT_FOUND` — order not found |
| From Burger Point's client, update a Pizza House order | `PERMISSION_DENIED` |
| Mark an order `READY` while it is still `PLACED` | `FAILED_PRECONDITION` — invalid transition |
| Cancel an order that is already `ACCEPTED` | `FAILED_PRECONDITION` |

---

## 5. Order states

```
PLACED ──► ACCEPTED ──► PREPARING ──► READY
   │
   └─────► CANCELLED
```

Anything not on that diagram is rejected with `FAILED_PRECONDITION`. An order
can only be cancelled while it is still `PLACED`; once a restaurant has accepted
it, cancelling is refused.

---

## 6. If something goes wrong

| Symptom | Cause |
| ------- | ----- |
| `Method not found` | stale generated stubs — regenerate them; or you connected to another student's server on port 50051 |
| `ModuleNotFoundError: No module named 'grpc'` | using the system `python3` on RCE; use `~/HW3/venv/bin/python3` |
| `ModuleNotFoundError: food_ordering_pb2` | stubs never generated — see §1 |
| Clients on other nodes cannot connect | the server was bound to `localhost`; bind `0.0.0.0` |
| `failed to connect to all addresses` | wrong node name or port, or the server is not running |
| `Access denied by pam_slurm_adopt` | you tried to `ssh` to a node you do not hold |
| Tracking terminal shows nothing | that is correct until the restaurant changes the status |
