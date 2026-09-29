"""Interactive customer client for the Food Ordering system."""
import sys
import threading

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU = """
1. List Restaurants
2. Place Order
3. Check Order Status
4. Track Order
5. Cancel Order
6. Exit
"""


def list_restaurants(stub):
    response = stub.ListRestaurants(pb2.RestaurantRequest())
    for restaurant in response.restaurants:
        print(f"\n{restaurant.name}")
        for item in restaurant.items:
            print(f"  - {item.name} : {item.price}")


def place_order(stub, customer_id):
    restaurant_name = input("Restaurant name: ").strip()
    items = []
    while True:
        food_name = input("Food item (blank to finish): ").strip()
        if not food_name:
            break
        quantity = int(input("Quantity: ").strip())
        items.append(pb2.OrderItem(food_name=food_name, quantity=quantity))

    try:
        response = stub.PlaceOrder(
            pb2.OrderRequest(customer_id=customer_id, restaurant_name=restaurant_name, items=items)
        )
        print(f"Order placed. Order ID: {response.order_id}, Total: {response.total}, Status: {response.status}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def check_status(stub):
    order_id = input("Order ID: ").strip()
    try:
        response = stub.GetOrderStatus(pb2.OrderStatusRequest(order_id=order_id))
        print(f"Order {response.order_id} : {response.status}")
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def track_order(stub):
    order_id = input("Order ID: ").strip()

    def stream_updates():
        try:
            for update in stub.SubscribeToOrderUpdates(pb2.OrderTrackRequest(order_id=order_id)):
                print(f"\n[Update] Order {update.order_id} : {update.status}")
        except grpc.RpcError as e:
            print(f"[Error] {e.code().name}: {e.details()}")

    print(f"Tracking order {order_id}...")
    threading.Thread(target=stream_updates, daemon=True).start()


def cancel_order(stub):
    order_id = input("Order ID: ").strip()
    restaurant_name = input("Restaurant name: ").strip()
    try:
        response = stub.UpdateOrderStatus(
            pb2.OrderStatusUpdate(order_id=order_id, restaurant_name=restaurant_name, new_status="CANCELLED")
        )
        print(response.message)
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def run(address):
    channel = grpc.insecure_channel(address)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)
    customer_id = input("Enter customer ID: ").strip()

    actions = {
        "1": lambda: list_restaurants(stub),
        "2": lambda: place_order(stub, customer_id),
        "3": lambda: check_status(stub),
        "4": lambda: track_order(stub),
        "5": lambda: cancel_order(stub),
    }

    while True:
        print(MENU)
        choice = input("Choose an option: ").strip()
        if choice == "6":
            break
        action = actions.get(choice)
        if action:
            action()
        else:
            print("Invalid option")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python customer.py <server-host:port>")
        sys.exit(1)
    run(sys.argv[1])
