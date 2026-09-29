"""Interactive restaurant client for the Food Ordering system."""
import sys

import grpc

import food_ordering_pb2 as pb2
import food_ordering_pb2_grpc as pb2_grpc

MENU = """
1. View Pending Orders
2. Accept Order
3. Start Preparing
4. Mark Ready
5. Exit
"""


def view_pending_orders(stub, restaurant_name):
    response = stub.ListRestaurantOrders(pb2.RestaurantOrdersRequest(restaurant_name=restaurant_name))
    if not response.orders:
        print("No orders yet")
    for order in response.orders:
        print(f"Order {order.order_id} : {order.status} (Total: {order.total})")


def update_status(stub, restaurant_name, new_status):
    order_id = input("Order ID: ").strip()
    try:
        response = stub.UpdateOrderStatus(
            pb2.OrderStatusUpdate(order_id=order_id, restaurant_name=restaurant_name, new_status=new_status)
        )
        print(response.message)
    except grpc.RpcError as e:
        print(f"[Error] {e.code().name}: {e.details()}")


def run(address, restaurant_name):
    channel = grpc.insecure_channel(address)
    stub = pb2_grpc.FoodOrderingServiceStub(channel)

    actions = {
        "1": lambda: view_pending_orders(stub, restaurant_name),
        "2": lambda: update_status(stub, restaurant_name, "ACCEPTED"),
        "3": lambda: update_status(stub, restaurant_name, "PREPARING"),
        "4": lambda: update_status(stub, restaurant_name, "READY"),
    }

    while True:
        print(MENU)
        choice = input("Choose an option: ").strip()
        if choice == "5":
            break
        action = actions.get(choice)
        if action:
            action()
        else:
            print("Invalid option")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usage: python restaurant.py <server-host:port> "<restaurant name>"')
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
