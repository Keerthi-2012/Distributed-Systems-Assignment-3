python3 server.py 0.0.0.0:50051
# Food Ordering System (gRPC)

## Setup
```
pip install grpcio grpcio-tools
```

Regenerate stubs after editing the proto:
```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto
```

## Run locally
```
python server.py localhost:50051
python customer.py localhost:50051
python restaurant.py localhost:50051 "Pizza House"
```

## Run on the RCE cluster
Local commands above are for development only. For the cluster demo, follow the
separate RCE Cluster Execution Guide (`salloc`, per-node `ssh`, server bound to
`0.0.0.0:<port>`, clients pointed at `<server-node>:<port>` instead of `localhost`).

## Demo walkthrough
1. Customer: `List Restaurants` -> shows Pizza House / Burger Point menus.
2. Customer: `Place Order` on Pizza House -> gets order ID, total, status `PLACED`.
3. Restaurant: `View Pending Orders` -> shows the new order; `Accept Order`,
   `Start Preparing`, `Mark Ready` in sequence.
4. Customer: `Track Order` -> receives `ACCEPTED`, `PREPARING`, `READY` live via streaming.
5. Run two clients concurrently (e.g. two customer terminals, or a customer + a
   restaurant terminal) against the same server to show concurrent handling.

## Exception cases demonstrated
- Placing an order for a restaurant/item that doesn't exist -> `NOT_FOUND`.
- Restaurant retrying an already-applied transition (e.g. `Mark Ready` twice) -> `FAILED_PRECONDITION`.
- A restaurant trying to update another restaurant's order -> `PERMISSION_DENIED`.

## Design notes
- `ListRestaurantOrders` is a helper RPC (not in the original spec's client menu)
  added so a restaurant client can list its own pending orders without extra state
  tracking on the client side.
- Cancellation is implemented via `UpdateOrderStatus` with status `CANCELLED`,
  only allowed while an order is still `PLACED`.
