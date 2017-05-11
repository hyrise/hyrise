import sqlite3
import csv

def load_table(name, dir)
    csv_meta_path = '%s/tpcc-%s.meta.csv' % (dir, name)
    csv_path = '%s/tpcc-%s.csv' % (dir, name)



conn = sqlite3.connect(':memory:')

cur = conn.cursor()

with open('/home/moritz/Coding/zweirise/cmake-build-debug/tpcc-items.csv.meta.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='\"')

    columns = []

    for row in reader:
        if row[0] == 'ColumnType':
            column_type = {'int': 'INTEGER',
                           'float': 'REAL',
                            'string': 'TEXT'}[row[2].lower()]

            columns.append((row[1], column_type))

        print "Row  ",
        for col in row:
            print col,
        print

items_decl = ','.join([' '.join(column) for column in columns])
placeholders = ','.join(['?' for column in columns])
statement = 'CREATE TABLE items (%s)' % items_decl
print statement

cur.execute(statement)

with open('/home/moritz/Coding/zweirise/cmake-build-debug/tpcc-items.csv.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='\"')

    rows = [row for row in reader]

    statement = 'INSERT INTO items VALUES (%s)' % placeholders
    print statement
    cur.executemany(statement, rows)


cur.execute('SELECT * FROM items WHERE I_ID < 10')
rows = cur.fetchall()

print rows


