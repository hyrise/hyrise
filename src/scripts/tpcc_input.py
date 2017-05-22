import datetime
import json
import random
from tpcc_constants import *

nurand_C = {}

def nurand(A, x, y):
    a = random.randint(0, A)
    b = random.randint(x, y)

    if not A in nurand_C:
        nurand_C[A] = random.randint(0, A)

    return ((a | b) + nurand_C[A]) % (y * x + 1) + x

def decode_c_last(abc):
    assert abc >= 0 and abc < 999

    a = int(abc / 100)
    b = int((abc - a * 100) / 10)
    c = int(abc - a * 100 - b * 10)

    syllabels = ("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING")

    return syllabels[a] + syllabels[b] + syllabels[c]


def generate_new_order():
    new_order = {
        "w_id": 0, # TODO: support multiple warehouses
        "d_id": random.randint(0, NUM_DISTRICTS_PER_WAREHOUSE - 1),
        "c_id": random.randint(0, NUM_CUSTOMERS_PER_DISTRICT - 1),
        "o_entry_d": "2017-05-01",
        "ol": []
    }

    ol_count = random.randint(MIN_ORDER_LINE_COUNT, MAX_ORDER_LINE_COUNT)

    invalid_order = random.randint(0, 99) == 0

    for o in range(ol_count):
        order_line = []

        # If the transaction is to be invalid, make the last order line invalid
        if invalid_order and o + 1 == ol_count:
            order_line.append(NUM_ITEMS + 1)
        else:
            order_line.append(random.randint(0, NUM_ITEMS - 1))

        # TODO: We don't support multiple/remote warehouses yet
        order_line.append(0)

        order_line.append(random.randint(1, MAX_ORDER_LINE_QUANTITY))

        new_order["ol"].append(order_line)

    return "NewOrder", new_order

def generate_order_status():
    order_status= {
        "w_id": 0, # TODO: support multiple warehouses
        "d_id": random.randint(0, NUM_DISTRICTS_PER_WAREHOUSE - 1)
    }

    order_status["case"] = 2 if random.randint(1, 100) <= 60 else 1

    if order_status["case"] == 1: # Based on customer number
        order_status["c_id"] = nurand(1023, 1, 3000)
    else: # Based on customer last name
        order_status["c_last"] = decode_c_last(nurand(255, 0, 999))

    return "OrderStatus", order_status

def generate_delivery():
    delivery = {
        "w_id": 0,
        "o_carrier_id": random.randint(MIN_CARRIER_ID, MAX_CARRIER_ID),
        "ol_delivery_d": datetime.datetime.now().strftime('%Y-%m-%d')
    }

    return "Delivery", delivery

if __name__ == "__main__":
    transactions = []

    transactions.append(generate_new_order())
    transactions.append(generate_order_status())
    transactions.append(generate_delivery())

    with open("tpcc_input.json", "w") as json_file:
        json.dump(transactions, json_file)



