import sqlite3
import csv
import json
import sqlitedriver.TXN_QUERIES as tpcc_queries

def execute_sql(cur, statement):
    print statement
    cur.execute(statement)

def executemany_sql(cur, statement, params):
    print statement
    cur.executemany(statement, params)


def load_table(cur, name, dir):
    csv_meta_path = '%s/tpcc-%s.meta.csv' % (dir, name)
    csv_path = '%s/tpcc-%s.csv' % (dir, name)
    cur = conn.cursor()

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
    execute_sql('CREATE TABLE items (%s)' % items_decl)

    with open(csv_path) as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='\"')

        rows = [row for row in reader]

        executemany_sql('INSERT INTO items VALUES (%s)' % placeholders, rows)

def init_db():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()

    csv_path = "/home/moritz/Coding/zweirise/cmake-build-debug"

    load_table(cur, csv_path, "warehouse")
    load_table(cur, csv_path, "district")
    load_table(cur, csv_path, "customer")
    load_table(cur, csv_path, "orders")
    load_table(cur, csv_path, "new_order")
    load_table(cur, csv_path, "item")
    load_table(cur, csv_path, "stock")
    load_table(cur, csv_path, "order_line")

    return cur

def process_new_orders(cur, new_orders_results, new_orders):
    new_order_queries = tpcc_queries["NEW_ORDER"]

    for new_order in new_orders:
        w_id = new_order["WarehouseId"]
        d_id = new_order["DistrictId"]
        c_id = new_order["CustomerId"]
        o_entry_d = new_order["OrderEntryDate"]
        o_carrier_id = 0 # TODO
        ol_cnt = len(new_order["OrderLines"])
        all_local = True # TODO once/if we support multiple warehouses

        cur.execute(new_order_queries["getWarehouseTaxRate"], [w_id])
        w_tax_rate = cur.fetchone()[0]

        cur.execute(new_order_queries["getDistrict"], [w_id, d_id])
        district = cur.fetchone()
        d_tax_rate = district[0]
        d_next_o_id = district[1]

        cur.execute(new_order_queries["getCustomer"], [w_id, d_id, c_id])
        c_discount = cur.fetchone()[0]

        cur(new_order_queries["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
        cur(new_order_queries["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
        cur(new_order_queries["createNewOrder"], [d_next_o_id, d_id, w_id])

        stock_infos = []

        for order_line in new_order["OrderLines"]:
            cur(new_order_queries["getStockInfo"])


        transaction_result = {
            "WarehouseTaxRate": w_tax_rate,
            "District": [d_tax_rate, d_next_o_id],
            "Customer": c_discount,
            "StockInfos": stock_infos
        }








conn = sqlite3.connect(':memory:')
cur = conn.cursor()

tpcc_input_path = "/home/moritz/Coding/zweirise/src/scripts/tpcc_input.json"

query_results = {
    "NewOrders": []
}

with open(tpcc_input_path) as tpcc_input_file:
    tpcc_input = json.load(tpcc_input_file)

cur = init_db()

process_new_orders(init_db(), query_results["NewOrders"], tpcc_input["NewOrders"])







