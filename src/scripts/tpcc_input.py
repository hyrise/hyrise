import json
import random

NUM_DISTRICTS = 10
NUM_CUSTOMERS_PER_DISTRICT = 3000
MIN_ORDER_LINE_COUNT = 5
MAX_ORDER_LINE_COUNT = 15
NUM_ITEMS = 100000
MAX_ORDER_LINE_QUANTITY = 10

def generate_new_order(input_dict):
    new_order = {
        "WarehouseId": 0,
        "DistrictId": random.randint(0, NUM_DISTRICTS - 1),
        "CustomerId": random.randint(0, NUM_CUSTOMERS_PER_DISTRICT - 1),
        "OrderEntryDate": "2017-05-01",
        "OrderLines": []
    }

    ol_count = random.randint(MIN_ORDER_LINE_COUNT, MAX_ORDER_LINE_COUNT)

    invalid_order = random.randint(0, 99) == 0

    for o in ol_count:
        order_line = []

        # If the transaction is to be invalid, make the last order line invalid
        if invalid_order and o + 1 == new_order["OrderLineCount"]:
            order_line.append(NUM_ITEMS + 1)
        else:
            order_line.append(random.randint(0, NUM_ITEMS - 1))

        # TODO: We don't support multiple/remote warehouses yet
        order_line.append(0)

        order_line.append(random.randint(1, MAX_ORDER_LINE_QUANTITY))

        new_order["OrderLines"].append(order_line)

    input_dict["NewOrders"].append(new_order)



if __name__ == "__main__":
    input_dict = {
        "NewOrders": []
    }

    for o in range(100):
        generate_new_order(input_dict)

    with open("tpcc_input.json", "w") as json_file:
        json.dump(input_dict, json_file)



