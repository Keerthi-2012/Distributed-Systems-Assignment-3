# Section 3 Problem 2 — Food Ordering System (gRPC)

A central server holds a fixed set of restaurants and the state of every order.
Customer clients place and track orders; restaurant clients receive them and
advance their status. Built with gRPC, Python 3.

Section 3 of the assignment offers two problems — Collaborative Document Editing
and this one. **This is Problem 2.**

Every transcript below is real output, captured from a run of this code. Order
ids are handed out in order from a freshly started server, so if you follow the
steps from a fresh `server.py` you will see exactly these ids.

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
| `UpdateOrderStatus` | unary | a restaurant advances an order, or a customer cancels |
| `SubscribeToOrderUpdates` | **server-streaming** | a customer is pushed every status change |
| `ListRestaurantOrders` | unary | a restaurant lists its own orders (extra, beyond the required five) |

### Order states

```
PLACED ──► ACCEPTED ──► PREPARING ──► READY
   │
   └─────► CANCELLED
```

Anything not on that diagram is rejected. An order can be cancelled only while
still `PLACED`; once a restaurant has accepted it, cancelling is refused.

---

## 4. Starting the system

The server address is a command-line argument, as the assignment specifies.
Use three terminals.

**Terminal A — server**

```bash
python3 server.py localhost:50051
```

```
Server listening on localhost:50051
```

**Terminal B — customer**

```bash
python3 customer.py localhost:50051
```

It asks for a customer id once, then shows the menu before every choice:

```
Enter customer ID: C1

1. List Restaurants
2. Place Order
3. Check Order Status
4. Track Order
5. Cancel Order
6. Exit

Choose an option:
```

**Terminal C — restaurant.** The restaurant's name is the second argument, so a
client sees and controls only its own orders.

```bash
python3 restaurant.py localhost:50051 "Pizza House"
```

```
1. View Pending Orders
2. Accept Order
3. Start Preparing
4. Mark Ready
5. Exit

Choose an option:
```

---

## 5. The six required demonstrations

The menu block is reprinted before every prompt; below it is shown once and then
left out, so the transcripts stay readable.

### 5.1 Listing restaurants

Terminal B, option `1`:

```
Choose an option: 1

Pizza House
  - Margherita Pizza : 250
  - Farmhouse Pizza : 350
  - Garlic Bread : 150

Burger Point
  - Veg Burger : 180
  - Cheese Burger : 220
  - French Fries : 120
```

### 5.2 Placing an order

Terminal B, option `2`. Items are entered one at a time; a blank food name ends
the order. The server prices it and returns a unique id.

```
Choose an option: 2
Restaurant name: Pizza House
Food item (blank to finish): Margherita Pizza
Quantity: 2
Food item (blank to finish): Garlic Bread
Quantity: 1
Food item (blank to finish):
Order placed. Order ID: O1, Total: 650, Status: PLACED
```

650 is `250 x 2 + 150 x 1`, computed on the server from its own menu — the
client never sends a price.

Option `3` reads the status back:

```
Choose an option: 3
Order ID: O1
Order O1 : PLACED
```

### 5.3 A restaurant receiving and processing the order

Terminal C. Option `1` lists the orders belonging to this restaurant:

```
Choose an option: 1
Order O1 : PLACED (Total: 650)
Order O2 : PLACED (Total: 350)
```

Options `2`, `3` and `4` walk the order forward, each asking for the id:

```
Choose an option: 2
Order ID: O2
Order O2 : ACCEPTED

Choose an option: 3
Order ID: O2
Order O2 : PREPARING

Choose an option: 4
Order ID: O2
Order O2 : READY
```

### 5.4 A customer receiving real-time updates

This is the important one. **Before** the restaurant does anything, the customer
chooses option `4` and gives the order id:

```
Choose an option: 4
Order ID: O2
Tracking order O2...
```

Tracking runs on a background thread, so the customer menu stays usable. As the
restaurant performs the three updates in 5.3, this terminal prints each one **by
itself**, with nothing typed:

```
[Update] Order O2 : ACCEPTED

[Update] Order O2 : PREPARING

[Update] Order O2 : READY
```

That is `SubscribeToOrderUpdates`, the server-streaming RPC. The customer asked
once; the server pushed three messages over the one open stream. Nothing polls,
and the stream closes on its own when the order reaches `READY` or `CANCELLED`.

### 5.5 Two clients interacting concurrently

Open a fourth terminal with a second customer and order while the first is still
tracking. Both are served at once and the ids differ.

Pushed harder — 20 customers placing orders at the same instant:

```
20 customers ordered at the same time
  unique order ids : 20 of 20
```

No two customers were given the same id, because the id counter and the order
table are only ever touched while holding the server's lock.

And with many customers tracking at once, the server still answers ordinary
calls immediately:

```
40 customers tracking orders at the same time
  ListRestaurants still answered in 0.00 s (2 restaurants)
```

That second check is not decoration — see the thread-pool note in §6.

### 5.6 Exception cases with gRPC status codes

Each line below is the real error, captured from the running server:

```
order from 'Nonexistent Cafe'              -> NOT_FOUND: Restaurant not found
order 'Sushi' from Pizza House             -> NOT_FOUND: Item not available: Sushi
status of unknown order ZZZ                -> NOT_FOUND: Order not found
track an unknown order YYY                 -> NOT_FOUND: Order not found
Burger Point updates a Pizza House order   -> PERMISSION_DENIED: Order belongs to another restaurant
mark O1 READY while still PLACED           -> FAILED_PRECONDITION: Invalid order state transition
cancel an order already READY              -> FAILED_PRECONDITION: Invalid order state transition
```

To reproduce them from the CLIs:

| Do this | Where | Result |
| ------- | ----- | ------ |
| Place an order at `Nonexistent Cafe` | customer, option 2 | `NOT_FOUND` |
| Order `Sushi` from Pizza House | customer, option 2 | `NOT_FOUND` |
| Check status of order `ZZZ` | customer, option 3 | `NOT_FOUND` |
| Track order `YYY` | customer, option 4 | `NOT_FOUND` |
| From Burger Point's terminal, update a Pizza House order | restaurant, option 2 | `PERMISSION_DENIED` |
| Mark an order `READY` while still `PLACED` | restaurant, option 4 | `FAILED_PRECONDITION` |
| Cancel an order already accepted | customer, option 5 | `FAILED_PRECONDITION` |

The client prints them as `[Error] CODE: details`, for example:

```
Choose an option: 2
Restaurant name: Nonexistent Cafe
Food item (blank to finish): Veg Burger
Quantity: 1
Food item (blank to finish):
[Error] NOT_FOUND: Restaurant not found
```

The three codes mean different things, and a client can act differently on each:
`NOT_FOUND` says the thing does not exist, `PERMISSION_DENIED` says it exists but
is not yours, and `FAILED_PRECONDITION` says it exists and is yours but the
system is in the wrong state to do that now.

**Cancellation**, for contrast, is allowed from `PLACED` and succeeds — customer
option `5`:

```
Choose an option: 5
Order ID: O3
Restaurant name: Burger Point
Order O3 : CANCELLED

Choose an option: 3
Order ID: O3
Order O3 : CANCELLED
```

Trying the same thing once the restaurant has accepted the order gives
`FAILED_PRECONDITION`, which is the last row of the table above.

### On the cluster

Same thing across machines — the server binds `0.0.0.0` so other nodes can reach
it, and clients are given `<server-node>:<port>`.

Three rules first:

1. **Never run servers on the login node.** Hold machines with `salloc`.
2. **You can only `ssh` to a node you currently hold**, or you get
   `Access denied by pam_slurm_adopt`.
3. **Do not use port 50051.** It is the example port in the RCE guide, so every
   student tries to bind it. If someone else already holds it your server dies
   and your client silently connects to *theirs*, which shows up as
   `Method not found` rather than as an error you would recognise. Derive a port
   from your user id instead.

```bash
ssh <your-username>@rce.iiit.ac.in
salloc --nodes=3 --ntasks-per-node=1 --cpus-per-task=4 --time=01:00:00
scontrol show hostnames $SLURM_JOB_NODELIST      # e.g. node01 node02 node03
PORT=$((50000 + $(id -u) % 9000))
echo $PORT
```

**Terminal A — the server, on the first node.** Bind `0.0.0.0`; `localhost`
would accept connections only from that node itself:

```bash
ssh node01
cd ~/HW3/Section3_Q2
~/HW3/venv/bin/python3 server.py 0.0.0.0:$PORT
```

**Terminal B — a customer, on the second node:**

```bash
ssh node02
cd ~/HW3/Section3_Q2
~/HW3/venv/bin/python3 customer.py node01:$PORT
```

**Terminal C — a restaurant, on the third node:**

```bash
ssh node03
cd ~/HW3/Section3_Q2
~/HW3/venv/bin/python3 restaurant.py node01:$PORT "Pizza House"
```

A fourth terminal with a second customer shows two clients interacting with the
server at the same time.

### If something goes wrong

| Symptom | Cause |
| ------- | ----- |
| `Method not found` | stale generated stubs — regenerate them; or you connected to another student's server on port 50051 |
| `ModuleNotFoundError: No module named 'grpc'` | using the system `python3` on RCE; use `~/HW3/venv/bin/python3` |
| `ModuleNotFoundError: food_ordering_pb2` | stubs never generated — see §2 |
| Clients on other nodes cannot connect | the server was bound to `localhost`; bind `0.0.0.0` |
| `failed to connect to all addresses` | wrong node name or port, or the server is not running |
| `Access denied by pam_slurm_adopt` | you tried to `ssh` to a node you do not hold |
| Tracking terminal shows nothing | that is correct until the restaurant changes the status |

---

## 6. Design notes

**Concurrency.** All shared state — the order table, the id counter and the
subscriber lists — is protected by a single lock. The critical sections are a
dictionary lookup and an assignment, so one lock costs nothing; per-order locks
would add a way to deadlock in exchange for contention that does not exist at
this scale. The 20-of-20 unique ids in §5.5 is this lock being measured.

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
is 64 workers. That is what the 40-tracker measurement in §5.5 is checking, and
it now answers in 0.00 s.

**`ListRestaurantOrders`** is an extra RPC beyond the specified five, added so a
restaurant client can list its own pending orders without tracking state itself.

**Cancellation** is expressed through `UpdateOrderStatus` with status
`CANCELLED`, permitted only from `PLACED`.
