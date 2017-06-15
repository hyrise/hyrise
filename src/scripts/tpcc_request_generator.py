import datetime
import json
import random
import argparse
from tpcc_constants import *

nurand_C = {}


def nurand(A, x, y):
    a = random.randint(0, A)
    b = random.randint(x, y)

    if not A in nurand_C:
        nurand_C[A] = random.randint(0, A)

    return ((a | b) + nurand_C[A]) % (y - x + 1) + x


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
        "o_entry_d": 0, #TODO: Enter real data here
        "order_lines": []
    }

    ol_count = random.randint(MIN_ORDER_LINE_COUNT, MAX_ORDER_LINE_COUNT)

    invalid_order = random.randint(0, 99) == 0

    for o in range(ol_count):
        # TPCC-DEVIATION: We should generate occasional invalid requests here
        i_id = random.randint(0, NUM_ITEMS - 1)

        new_order["order_lines"].append({
            "i_id": i_id,
            "w_id": 0,  # TODO: We don't support multiple/remote warehouses yet
            "qty": random.randint(1, MAX_ORDER_LINE_QUANTITY)
        })

    return {"transaction": "NewOrder",
            "params": new_order}

def generate_order_status():
    order_status= {
        "c_w_id": 0, # TODO: support multiple warehouses
        "c_d_id": random.randint(0, NUM_DISTRICTS_PER_WAREHOUSE - 1)
    }

    order_status["case"] = 2 if random.randint(1, 100) <= 60 else 1

    if order_status["case"] == 1: # Based on customer number
        order_status["c_id"] = nurand(1023, 1, 3000)
    else: # Based on customer last name
        order_status["c_last"] = decode_c_last(nurand(255, 0, 999))

    return {"transaction": "OrderStatus",
            "params": order_status}


def generate_delivery():
    delivery = {
        "w_id": 0,
        "o_carrier_id": random.randint(MIN_CARRIER_ID, MAX_CARRIER_ID),
        "ol_delivery_d": datetime.datetime.now().strftime('%Y-%m-%d')
    }

    return {"transaction": "Delivery",
            "params": delivery}


def generate_requests(distribution, num_requests=100):
    transactions = []

    if distribution == 'test':
        for i in range(num_requests):
            request_type = i % 2

            if request_type == 0:
                transactions.append(generate_new_order())
            if request_type == 1:
                transactions.append(generate_order_status())
    elif distribution == 'benchmark':
        while len(transactions) < num_requests:
            x = random.randint(1, 100)
            if x <= 4:  # 4%
                pass # Stock Level
            elif x <= 4 + 4: # 4%
                pass # DELIVERY
            elif x <= 4 + 4 + 4:  # 4%
                transactions.append(generate_order_status())
            elif x <= 43 + 4 + 4 + 4:  # 43%
                pass # PAYMENT
            else:  # 45%
                assert x > 100 - 45
                transactions.append(generate_order_status())
    else:
        print("ERROR: Unknown distribution {}".format(distribution))

    with open("tpcc_{}_requests.json".format(distribution), "w") as json_file:
        json.dump(transactions, json_file)


if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Generates a JSON file containing TPCC requests')
    aparser.add_argument('distribution', choices=['test', 'benchmark'],
                         help='For \'test\', generate even distribution of tests, for \'benchmark\' stick to the distribution specified by TPCC')
    aparser.add_argument('num_requests', type=int,
                         help='Number of requests to generate', default=100)
    args = vars(aparser.parse_args())

    print('Generating {} requests with \'{}\' distribution'.format(args['num_requests'], args['distribution']))
    generate_requests(args['distribution'], args['num_requests'])
    print('Done')

