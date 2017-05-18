import sqlite3
import csv
import json
from sqlitedriver import TXN_QUERIES as tpcc_queries
from tpcc_constants import *

def execute_sql(cur, statement, params=()):
    #print(statement, params)
    cur.execute(statement, params)

def executemany_sql(cur, statement, params=()):
    #print(statement)
    cur.executemany(statement, params)

def load_table(cur, dir, name, name_override=None):
    csv_meta_path = '%s/%s.meta.csv' % (dir, name)
    csv_path = '%s/%s.csv' % (dir, name)

    in_db_name = name_override if name_override != None else name

    with open(csv_meta_path) as csv_file:
        reader = csv.reader(csv_file, delimiter=',', quotechar='\"')

        columns = []

        for row in reader:
            if row[0] == 'ColumnType':
                column_type = {'int': 'INTEGER',
                               'float': 'REAL',
                               'string': 'TEXT'}[row[2].lower()]
                columns.append((row[1], column_type))

    items_decl = ','.join([' '.join(column) for column in columns])
    placeholders = ','.join(['?' for column in columns])
    execute_sql(cur, 'CREATE TABLE %s (%s)' % (in_db_name, items_decl))

    with open(csv_path) as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='\"')

        rows = [row for row in reader]

        executemany_sql(cur, 'INSERT INTO %s VALUES (%s)' % (in_db_name, placeholders), rows)

def init_db():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()

    csv_path = "/home/moritz/Coding/zweirise/"

    load_table(cur, csv_path, "WAREHOUSE")
    load_table(cur, csv_path, "DISTRICT")
    load_table(cur, csv_path, "CUSTOMER")
    load_table(cur, csv_path, "ORDER", "ORDERS")
    load_table(cur, csv_path, "NEW-ORDER", "NEW_ORDER")
    load_table(cur, csv_path, "ITEM")
    load_table(cur, csv_path, "STOCK")
    load_table(cur, csv_path, "ORDER-LINE", "ORDER_LINE")

    return cur

def process_new_order(cur, params):
    new_order_queries = tpcc_queries["NEW_ORDER"]

    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = params["c_id"]
    o_entry_d = params["o_entry_d"]
    o_carrier_id = 0 # TODO
    ol_cnt = len(params["ol"])
    all_local = True # TODO once/if we support multiple warehouses

    # Get Warehouse Tax Rate
    execute_sql(cur, new_order_queries["getWarehouseTaxRate"], [w_id])
    w_tax_rate = cur.fetchone()[0]

    # Get District
    execute_sql(cur, new_order_queries["getDistrict"], [d_id, w_id])
    district = cur.fetchone()
    d_tax_rate = district[0]
    d_next_o_id = district[1]

    execute_sql(cur, new_order_queries["getCustomer"], [w_id, d_id, c_id])
    costumer = cur.fetchone()
    c_discount = costumer[0]
    c_last = costumer[1]
    c_credit = costumer[2]

    execute_sql(cur, new_order_queries["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
    execute_sql(cur, new_order_queries["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
    execute_sql(cur, new_order_queries["createNewOrder"], [d_next_o_id, d_id, w_id])

    order_lines = []

    # total = 0

    for ol_idx, order_line in enumerate(params["ol"]):
        ol_i_id = order_line[0]
        ol_i_w_id = order_line[1]
        ol_i_qty = order_line[2]

        execute_sql(cur, new_order_queries["getItemInfo"], [ol_i_id])
        item = cur.fetchone()
        i_price = item[0]
        i_name = item[1]
        i_data = item[2]

        execute_sql(cur, new_order_queries["getStockInfo"] % (d_id + 1), [ol_i_id, ol_i_w_id])
        stock_info = cur.fetchone()
        s_qty = stock_info[0]
        s_data = stock_info[1]
        s_ytd = stock_info[2]
        s_order_cnt = stock_info[3]
        s_remote_cnt = stock_info[4]
        s_dist_xx = stock_info[5]

        # Dec stock, stock up
        s_ytd += ol_i_qty

        if s_qty >= ol_i_qty + 10:
            s_qty -= ol_i_qty
        else:
            s_qty += 91 - ol_i_qty

        s_order_cnt += 1

        execute_sql(cur, new_order_queries["updateStock"], [s_qty, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_i_w_id])

        if i_data.find(ORIGINAL_STRING) != -1 and s_data.find(ORIGINAL_STRING) != -1:
            brand_generic = 'B'
        else:
            brand_generic = 'G'

        ol_amount = ol_i_qty * i_price
        # total += ol_amount

        execute_sql(cur, new_order_queries["createOrderLine"],
                            [d_next_o_id, d_id, w_id, ol_idx, ol_i_id, ol_i_w_id, o_entry_d, ol_i_qty,
                             ol_amount, s_dist_xx])

        order_line = {
            "Item": [i_price, i_name, i_data],
            "StockInfo": [s_qty, s_dist_xx, s_ytd, s_order_cnt, s_remote_cnt, s_data]
        }

        order_lines.append(order_line)

    return {
        "WarehouseTaxRate": w_tax_rate,
        "District": [d_tax_rate, d_next_o_id],
        "Customer": [c_discount, c_last, c_credit],
        "OrderLines": order_lines
    }

def process_order_status(cur, params):
    q = tpcc_queries["ORDER_STATUS"]

    w_id = params["w_id"]
    d_id = params["d_id"]

    if params["case"] == 1:
        c_id = params["c_id"]
        execute_sql(cur, q["getCustomerByCustomerId"], (w_id, d_id, c_id))
        customer = cur.fetchone()
    else:
        c_last = params["c_last"]
        execute_sql(cur, q["getCustomersByLastName"], (w_id, d_id, c_last))
        customers = cur.fetchall()
        customer = customers[len(customers) / 2]

    c_id = customer[0]
    c_first = customer[1]
    c_middle = customer[2]
    c_last = customer[3]
    c_balance = customer[4]

    execute_sql(cur, q["getLastOrder"], (w_id, d_id, c_id))
    order = cur.fetchone()
    o_id = order[0]
    o_carrier_id = order[1]
    o_entry_d = order[2]

    execute_sql(cur, q["getOrderLines"], (w_id, d_id, o_id))
    order_lines = cur.fetchall() # (ol_supply_w_id, ol_i_id, ol_quantity, ol_amount, ol_delivery_d)1

    return {
        "Customer": (c_id, c_first, c_middle, c_last, c_balance),
        "Order": (o_id, o_carrier_id, o_entry_d),
        "OrderLines": order_lines
    }

if __name__ == "__main__":
    cur = init_db()

    tpcc_input_path = "/home/moritz/Coding/zweirise/src/scripts/tpcc_input.json"
    tpcc_output_path = "/home/moritz/Coding/zweirise/src/scripts/tpcc_output.json"

    query_results = []

    with open(tpcc_input_path) as tpcc_input_file:
        tpcc_input = json.load(tpcc_input_file)

    transaction_dispatch = {
        "NewOrder": process_new_order,
        "OrderStatus": process_order_status
    }

    for transaction_type, params in tpcc_input:
        query_results.append(transaction_dispatch[transaction_type](cur, params))

    with open(tpcc_output_path, "w") as tpcc_output_file:
        json.dump(query_results, tpcc_output_file)







