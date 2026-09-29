"""gRPC Food Ordering server."""
import sys
import queue
import threading
from concurrent import futures

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

# static restaurant -> {food name: price} catalog
RESTAURANTS = {
    "Pizza House": {
        "Margherita Pizza": 250,
        "Farmhouse Pizza": 350,
        "Garlic Bread": 150,
    },
    "Burger Point": {
        "Veg Burger": 180,
        "Cheese Burger": 220,
        "French Fries": 120,
    },
}

# allowed order status transitions
TRANSITIONS = {
    "PLACED": {"ACCEPTED", "CANCELLED"},
    "ACCEPTED": {"PREPARING"},
    "PREPARING": {"READY"},
    "READY": set(),
    "CANCELLED": set(),
}


class FoodOrderingServicer(pb2_grpc.FoodOrderingServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.orders = {}  # order_id -> dict(restaurant, items, total, status)
        self.next_order_id = 1
        self.subscribers = {}  # order_id -> list of Queue

    def ListRestaurants(self, request, context):
        restaurants = [
            pb2.Restaurant(
                name=name,
                items=[pb2.FoodItem(name=item, price=price) for item, price in menu.items()],
            )
            for name, menu in RESTAURANTS.items()
        ]
        return pb2.RestaurantResponse(restaurants=restaurants)

    def PlaceOrder(self, request, context):
        menu = RESTAURANTS.get(request.restaurant_name)
        if menu is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Restaurant not found")

        total = 0
        for item in request.items:
            if item.food_name not in menu:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Item not available: {item.food_name}")
            total += menu[item.food_name] * item.quantity

        with self.lock:
            order_id = f"O{self.next_order_id}"
            self.next_order_id += 1
            self.orders[order_id] = {
                "restaurant": request.restaurant_name,
                "customer_id": request.customer_id,
                "items": list(request.items),
                "total": total,
                "status": "PLACED",
            }

        return pb2.OrderResponse(order_id=order_id, total=total, status="PLACED")

    def GetOrderStatus(self, request, context):
        with self.lock:
            order = self.orders.get(request.order_id)
            if order is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Order not found")
            return pb2.OrderStatusResponse(order_id=request.order_id, status=order["status"])

    def UpdateOrderStatus(self, request, context):
        with self.lock:
            order = self.orders.get(request.order_id)
            if order is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Order not found")

            if order["restaurant"] != request.restaurant_name:
                context.abort(grpc.StatusCode.PERMISSION_DENIED, "Order belongs to another restaurant")

            current = order["status"]
            if request.new_status not in TRANSITIONS.get(current, set()):
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Invalid order state transition")

            order["status"] = request.new_status
            subscribers = list(self.subscribers.get(request.order_id, []))

        # notify subscribers outside the lock to avoid blocking other requests
        update = pb2.OrderUpdate(order_id=request.order_id, status=request.new_status)
        for subscriber_queue in subscribers:
            subscriber_queue.put(update)

        return pb2.Acknowledge(success=True, message=f"Order {request.order_id} : {request.new_status}")

    def SubscribeToOrderUpdates(self, request, context):
        with self.lock:
            if request.order_id not in self.orders:
                context.abort(grpc.StatusCode.NOT_FOUND, "Order not found")
            subscriber_queue = queue.Queue()
            self.subscribers.setdefault(request.order_id, []).append(subscriber_queue)

        try:
            while context.is_active():
                # Wait with a TIMEOUT, never for ever. This call occupies one
                # thread of the server's pool for as long as the customer keeps
                # tracking, and a bare get() would hold that thread even while
                # nothing is happening. With enough customers tracking at once,
                # every thread would be parked here and the server would stop
                # answering ordinary calls: ListRestaurants and PlaceOrder would
                # simply time out. Waking up once a second lets us re-check
                # whether the customer is still connected and release the thread
                # when they are not.
                try:
                    update = subscriber_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield update
                if update.status in ("READY", "CANCELLED"):
                    break
        finally:
            with self.lock:
                # discard() rather than remove(): the order may already have
                # been cleaned up, and a missing entry is not an error here.
                waiting = self.subscribers.get(request.order_id, [])
                if subscriber_queue in waiting:
                    waiting.remove(subscriber_queue)

    def ListRestaurantOrders(self, request, context):
        with self.lock:
            summaries = [
                pb2.OrderSummary(order_id=oid, status=o["status"], total=o["total"])
                for oid, o in self.orders.items()
                if o["restaurant"] == request.restaurant_name
            ]
        return pb2.RestaurantOrdersResponse(orders=summaries)


# Each customer tracking an order holds one thread for as long as they watch,
# so the pool has to be large enough for every tracker PLUS the ordinary calls
# arriving at the same time. Ten was too few: ten customers tracking their
# orders used every thread, and the server stopped responding altogether.
MAX_WORKERS = 64


def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    pb2_grpc.add_FoodOrderingServiceServicer_to_server(FoodOrderingServicer(), server)
    server.add_insecure_port(address)
    server.start()
    print(f"Server listening on {address}")
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python server.py <host:port>")
        sys.exit(1)
    serve(sys.argv[1])
