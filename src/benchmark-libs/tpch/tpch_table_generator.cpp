#include "tpch_table_generator.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/value_column.hpp"

namespace tpch {

TableGenerator::TableGenerator() : _random_gen(RandomGenerator()), _text_field_gen(TextFieldGenerator(_random_gen)) {}

// TODO(anybody) chunk sizes and number of chunks might be tuned in generate_XYZ_table

template <typename T>
std::shared_ptr<opossum::ValueColumn<T>> TableGenerator::add_column(
    size_t cardinality, const std::function<T(size_t)> &generator_function) {
  tbb::concurrent_vector<T> column(cardinality);
  for (size_t i = 0; i < column.size(); i++) {
    column[i] = generator_function(i);
  }
  return std::make_shared<opossum::ValueColumn<T>>(std::move(column));
}

float TableGenerator::calculate_part_retailprice(size_t i) const {
  return (90000.f + (i % 200001) / 10.f + 100.f * (i % 1000)) / 100.f;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_suppliers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("S_SUPPKEY", "int", false);
  table->add_column("S_NAME", "string", false);
  table->add_column("S_ADDRESS", "string", false);
  table->add_column("S_NATIONKEY", "int", false);
  table->add_column("S_PHONE", "string", false);
  table->add_column("S_ACCTBAL", "float", false);
  table->add_column("S_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _scale_factor * _supplier_size;
  // S_SUPPKEY
  chunk.add_column(add_column<int>(table_size, [](size_t i) { return i; }));
  // S_NAME
  chunk.add_column(
      add_column<std::string>(table_size, [&](size_t i) { return "Supplier#" + _text_field_gen.fixed_length(i, 9); }));
  // S_ADDRESS
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.v_string(10, 40); }));
  // S_NATIONKEY
  auto nationkeys = add_column<int>(table_size, [&](size_t) { return _random_gen.number(0, 24); });
  chunk.add_column(nationkeys);
  // S_PHONE
  chunk.add_column(add_column<std::string>(
      table_size, [&](size_t i) { return _text_field_gen.phone_number((*nationkeys).get(i)); }));
  // S_ACCTBAL
  chunk.add_column(add_column<float>(table_size, [&](size_t) { return _random_gen.number(-99999, 999999) / 100.f; }));
  // S_COMMENT
  auto complaint_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);
  auto recommendation_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);
  chunk.add_column(add_column<std::string>(table_size, [&](size_t i) {
    std::string comment = _text_field_gen.text_string(25, 100);
    bool complaints = complaint_ids.find(i) != complaint_ids.end();
    if (complaints) {
      std::string replacement("Customer Complaints");
      size_t start_pos = _random_gen.number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    bool recommends = recommendation_ids.find(i) != recommendation_ids.end();
    if (recommends) {
      std::string replacement("Customer Recommends");
      size_t start_pos = _random_gen.number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    return comment;
  }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_parts_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("P_PARTKEY", "int", false);
  table->add_column("P_NAME", "string", false);
  table->add_column("P_MFGR", "string", false);
  table->add_column("P_BRAND", "string", false);
  table->add_column("P_TYPE", "string", false);
  table->add_column("P_SIZE", "int", false);
  table->add_column("P_CONTAINER", "string", false);
  table->add_column("P_RETAILPRICE", "float", false);
  table->add_column("P_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _scale_factor * _part_size;
  // P_PARTKEY
  chunk.add_column(add_column<int>(table_size, [](size_t i) { return i; }));
  // P_NAME
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.part_name(); }));
  // P_MFGR
  std::vector<std::string> manufacturers(table_size);
  for (size_t i = 0; i < manufacturers.size(); i++) {
    manufacturers[i] = std::to_string(_random_gen.number(1, 5));
  }
  chunk.add_column(add_column<std::string>(table_size, [&](size_t i) { return "Manufacturer#" + manufacturers[i]; }));
  // P_BRAND
  chunk.add_column(add_column<std::string>(
      table_size, [&](size_t i) { return "Brand#" + manufacturers[i] + std::to_string(_random_gen.number(1, 5)); }));
  // P_TYPE
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.part_type(); }));
  // P_SIZE
  chunk.add_column(add_column<int>(table_size, [&](size_t) { return _random_gen.number(1, 50); }));
  // P_CONTAINER
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.part_container(); }));
  // P_RETAILPRICE
  chunk.add_column(add_column<float>(table_size, [&](size_t i) { return calculate_part_retailprice(i); }));
  // P_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(5, 22); }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_partsupps_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("PS_PARTKEY", "int", false);
  table->add_column("PS_SUPPKEY", "int", false);
  table->add_column("PS_AVAILQTY", "int", false);
  table->add_column("PS_SUPPLYCOST", "float", false);
  table->add_column("PS_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _scale_factor * _part_size * _partsupp_size;
  // PS_PARTKEY
  chunk.add_column(add_column<int>(table_size, [&](size_t i) { return i % (_scale_factor * _part_size); }));
  // PS_SUPPKEY
  chunk.add_column(add_column<int>(table_size, [&](size_t i) {
    size_t ps_partkey = i % (_scale_factor * _part_size);
    size_t j = i / (_scale_factor * _part_size);
    size_t s = _scale_factor * _supplier_size;
    return (ps_partkey + (j * (s / 4 + ps_partkey / s))) % s;
  }));
  // PS_AVAILQTY
  chunk.add_column(add_column<int>(table_size, [&](size_t) { return _random_gen.number(1, 9999); }));
  // PS_SUPPLYCOST
  chunk.add_column(add_column<float>(table_size, [&](size_t) { return _random_gen.number(100, 100000) / 100.f; }));
  // PS_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(49, 198); }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_customers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("C_CUSTKEY", "int", false);
  table->add_column("C_NAME", "string", false);
  table->add_column("C_ADDRESS", "string", false);
  table->add_column("C_NATIONKEY", "int", false);
  table->add_column("C_PHONE", "string", false);
  table->add_column("C_ACCTBAL", "float", false);
  table->add_column("C_MKTSEGMENT", "string", false);
  table->add_column("C_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _scale_factor * _customer_size;
  // C_CUSTKEY
  chunk.add_column(add_column<int>(table_size, [](size_t i) { return i; }));
  // C_NAME
  chunk.add_column(
      add_column<std::string>(table_size, [&](size_t i) { return "Customer#" + _text_field_gen.fixed_length(i, 9); }));
  // C_ADDRESS
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.v_string(10, 40); }));
  // C_NATIONKEY
  auto nationkeys = add_column<int>(table_size, [&](size_t) { return _random_gen.number(0, 24); });
  chunk.add_column(nationkeys);
  // C_PHONE
  chunk.add_column(add_column<std::string>(
      table_size, [&](size_t i) { return _text_field_gen.phone_number((*nationkeys).get(i)); }));
  // C_ACCTBAL
  chunk.add_column(add_column<float>(table_size, [&](size_t) { return _random_gen.number(-99999, 999999) / 100.f; }));
  // C_MKTSEGMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.customer_segment(); }));
  // C_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(29, 116); }));

  table->add_chunk(std::move(chunk));

  return table;
}

TableGenerator::order_lines_type TableGenerator::generate_order_lines() {
  size_t total_order_size = _order_size * _scale_factor * _customer_size;
  std::vector<std::vector<OrderLine>> all_order_lines(total_order_size);
  for (auto &order_lines_per_order : all_order_lines) {
    size_t order_lines_size = _random_gen.number(1, 7);
    order_lines_per_order.resize(order_lines_size);
    size_t order_i = &order_lines_per_order - &all_order_lines[0];
    size_t orderkey = order_i / 8 * 32 + order_i % 8;
    size_t orderdate = _random_gen.number(_startdate, _enddate - 151 * _one_day);
    for (auto &order_line : order_lines_per_order) {
      order_line.orderkey = orderkey;
      order_line.partkey = _random_gen.number(0, _scale_factor * _part_size - 1);
      order_line.quantity = _random_gen.number(1, 50);
      float part_retailprice = calculate_part_retailprice(order_line.partkey);
      order_line.extendedprice = order_line.quantity * part_retailprice;
      order_line.discount = _random_gen.number(1, 10) / 100.f;
      order_line.tax = _random_gen.number(1, 8) / 100.f;
      order_line.orderdate = orderdate;
      order_line.shipdate = orderdate + _random_gen.number(1, 121) * _one_day;
      order_line.receiptdate = order_line.shipdate + _random_gen.number(1, 30) * _one_day;
      order_line.linestatus = order_line.shipdate > _currentdate ? "O" : "F";
    }
  }
  return std::make_shared<std::vector<std::vector<OrderLine>>>(all_order_lines);
}

std::shared_ptr<opossum::Table> TableGenerator::generate_orders_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("O_ORDERKEY", "int", false);
  table->add_column("O_CUSTKEY", "int", false);
  table->add_column("O_ORDERSTATUS", "string", false);
  table->add_column("O_TOTALPRICE", "float", false);
  table->add_column("O_ORDERDATE", "int", false);
  table->add_column("O_ORDERPRIORITY", "string", false);
  table->add_column("O_CLERK", "string", false);
  table->add_column("O_SHIPPRIORITY", "int", false);
  table->add_column("O_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _order_size * _scale_factor * _customer_size;
  // O_ORDERKEY
  chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines->at(i)[0].orderkey; }));
  // O_CUSTKEY
  chunk.add_column(add_column<int>(table_size, [&](size_t) {
    // O_CUSTKEY should be random between 0 and _scale_factor * _customer_size,
    // but O_CUSTKEY % 3 must not be 0 (only two third of the keys are used).
    size_t max_compacted_key = _scale_factor * _customer_size / 3 * 2 - 1;
    size_t compacted_key = _random_gen.number(0, max_compacted_key);
    return compacted_key / 2 * 3 + compacted_key % 2 + 1;
  }));
  // O_ORDERSTATUS
  chunk.add_column(add_column<std::string>(table_size, [&](size_t i) {
    auto lines = order_lines->at(i);
    bool all_f = std::all_of(lines.begin(), lines.end(), [](OrderLine line) { return line.linestatus == "F"; });
    if (all_f) {
      return "F";
    }
    bool all_o = std::all_of(lines.begin(), lines.end(), [](OrderLine line) { return line.linestatus == "O"; });
    if (all_o) {
      return "O";
    }
    return "P";
  }));
  // O_TOTALPRICE
  chunk.add_column(add_column<float>(table_size, [&](size_t i) {
    float sum = 0.f;
    for (auto &order_line : order_lines->at(i)) {
      sum += order_line.extendedprice * (1 + order_line.tax) * (1 - order_line.discount);
    }
    return sum;
  }));
  // O_ORDERDATE
  chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines->at(i)[0].orderdate; }));
  // O_ORDERPRIORITY
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.order_priority(); }));
  // O_CLERK
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) {
    size_t clerk_number = _random_gen.number(1, 1000);
    return "Clerk#" + _text_field_gen.fixed_length(clerk_number, 9);
  }));
  // O_SHIPPRIORITY
  chunk.add_column(add_column<int>(table_size, [](size_t) { return 0; }));
  // O_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(29, 116); }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_lineitems_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("L_ORDERKEY", "int", false);
  table->add_column("L_PARTKEY", "int", false);
  table->add_column("L_SUPPKEY", "int", false);
  table->add_column("L_LINENUMBER", "int", false);
  table->add_column("L_QUANTITY", "int", false);
  table->add_column("L_EXTENDEDPRICE", "float", false);
  table->add_column("L_DISCOUNT", "float", false);
  table->add_column("L_TAX", "float", false);
  table->add_column("L_RETURNFLAG", "string", false);
  table->add_column("L_LINESTATUS", "string", false);
  table->add_column("L_SHIPDATE", "int", false);
  table->add_column("L_COMMITDATE", "int", false);
  table->add_column("L_RECEIPTDATE", "int", false);
  table->add_column("L_SHIPINSTRUCT", "string", false);
  table->add_column("L_SHIPMODE", "string", false);
  table->add_column("L_COMMENT", "string", false);

  for (auto &order_lines_per_order : *order_lines) {
    size_t table_size = order_lines_per_order.size();
    auto chunk = opossum::Chunk(true);
    // L_ORDERKEY
    chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines_per_order[i].orderkey; }));
    // L_PARTKEY
    chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines_per_order[i].partkey; }));
    // L_SUPPKEY
    chunk.add_column(add_column<int>(table_size, [](size_t) { return 1234; }));
    // L_LINENUMBER
    chunk.add_column(add_column<int>(table_size, [](size_t i) { return i; }));
    // L_QUANTITY
    chunk.add_column(add_column<int>(table_size, [&](size_t i) {
      return order_lines_per_order[i].quantity;
    }));
    // L_EXTENDEDPRICE
    chunk.add_column(add_column<float>(table_size, [&](size_t i) { return order_lines_per_order[i].extendedprice; }));
    // L_DISCOUNT
    chunk.add_column(add_column<float>(table_size, [&](size_t i) { return order_lines_per_order[i].discount; }));
    // L_TAX
    chunk.add_column(add_column<float>(table_size, [&](size_t i) { return order_lines_per_order[i].tax; }));
    // L_RETURNFLAG
    chunk.add_column(add_column<std::string>(table_size, [&](size_t i) {
      if (order_lines_per_order[i].receiptdate <= _currentdate) {
        if (_random_gen.number(0, 1)) {
          return "R";
        } else {
          return "A";
        }
      } else {
        return "N";
      }
    }));
    // L_LINESTATUS
    chunk.add_column(
        add_column<std::string>(table_size, [&](size_t i) { return order_lines_per_order[i].linestatus; }));
    // L_SHIPDATE
    chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines_per_order[i].shipdate; }));
    // L_COMMITDATE
    chunk.add_column(add_column<int>(table_size, [&](size_t i) {
      auto orderdate = order_lines_per_order[i].orderdate;
      return orderdate + _random_gen.number(30, 90) * _one_day;
    }));
    // L_RECEIPTDATE
    chunk.add_column(add_column<int>(table_size, [&](size_t i) { return order_lines_per_order[i].receiptdate; }));
    // L_SHIPINSTRUCT
    chunk.add_column(
        add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.lineitem_instruction(); }));
    // L_SHIPMODE
    chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.lineitem_mode(); }));
    // L_COMMENT
    chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(10, 43); }));

    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_nations_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("N_NATIONKEY", "int", false);
  table->add_column("N_NAME", "string", false);
  table->add_column("N_REGIONKEY", "int", false);
  table->add_column("N_COMMENT", "string", false);

  auto chunk = opossum::Chunk();
  size_t table_size = _nation_size;
  // N_NATIONKEY
  chunk.add_column(add_column<int>(table_size, [](size_t i) { return i; }));
  // N_NAME
  chunk.add_column(add_column<std::string>(table_size, [](size_t) { return ""; }));
  // N_REGIONKEY
  chunk.add_column(add_column<int>(table_size, [](size_t) { return 0; }));
  // N_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(31, 114); }));

  table->add_chunk(std::move(chunk));

  return table;
}

void TableGenerator::add_all_tables(opossum::StorageManager &manager) {
  // auto supplier_table = generate_suppliers_table();
  // auto parts_table = generate_parts_table();
  // auto partsupps_table = generate_partsupps_table();
  // auto customers_table = generate_customers_table();
  // auto order_lines = generate_order_lines();
  // auto orders_table = generate_orders_table(order_lines);
  // auto lineitems_table = generate_lineitems_table(order_lines);
  auto nations_table = generate_nations_table();

  // manager.add_table("SUPPLIER", std::move(supplier_table));
  // manager.add_table("PART", std::move(parts_table));
  // manager.add_table("PARTSUPP", std::move(partsupps_table));
  // manager.add_table("CUSTOMER", std::move(customers_table));
  // manager.add_table("ORDERS", std::move(orders_table));
  // manager.add_table("LINEITEM", std::move(lineitems_table));
  manager.add_table("NATION", std::move(nations_table));
}

}  // namespace tpch
