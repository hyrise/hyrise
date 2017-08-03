#include "tpch_table_generator.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "storage/dictionary_compression.hpp"

namespace tpch {

TableGenerator::TableGenerator(const size_t chunk_size, const size_t scale_factor)
    : AbstractBenchmarkTableGenerator(1000),
      _chunk_size(chunk_size),
      _scale_factor(scale_factor),
      _random_gen(benchmark_utilities::RandomGenerator()),
      _text_field_gen(TextFieldGenerator(_random_gen)) {}

// TODO(anybody) chunk sizes and number of chunks might be tuned in generate_XYZ_table

float TableGenerator::calculate_part_retailprice(size_t partkey) const {
  return (90000.f + (partkey % 200001) / 10.f + 100.f * (partkey % 1000)) / 100.f;
}

int32_t TableGenerator::calculate_partsuppkey(size_t partkey, size_t supplier) const {
  size_t s = _scale_factor * NUM_SUPPLIERS;
  return (partkey + (supplier * (s / 4 + partkey / s))) % s;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_suppliers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * NUM_SUPPLIERS;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "S_SUPPKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<std::string>(table, "S_NAME", cardinalities, [&](std::vector<size_t> indices) {
    return "Supplier#" + _text_field_gen.pad_int_with_zeroes(indices[0], 9);
  });

  add_column<std::string>(table, "S_ADDRESS", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.v_string(10, 40); });

  std::vector<int> nationkeys(table_size);
  std::generate(nationkeys.begin(), nationkeys.end(), [&]() { return _random_gen.random_number(0, 24); });

  add_column<int>(table, "S_NATIONKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return nationkeys[indices[0]]; });

  add_column<std::string>(table, "S_PHONE", cardinalities, [&](std::vector<size_t> indices) {
    return _text_field_gen.generate_phone_number(nationkeys[indices[0]]);
  });

  add_column<float>(table, "S_ACCTBAL", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(-99999, 999999) / 100.f; });

  auto complaint_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);
  auto recommendation_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);

  add_column<std::string>(table, "S_COMMENT", cardinalities, [&](std::vector<size_t> indices) {
    std::string comment = _text_field_gen.text_string(25, 100);
    bool complaints = complaint_ids.find(indices[0]) != complaint_ids.end();
    if (complaints) {
      std::string replacement("Customer Complaints");
      size_t start_pos = _random_gen.random_number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    bool recommends = recommendation_ids.find(indices[0]) != recommendation_ids.end();
    if (recommends) {
      std::string replacement("Customer Recommends");
      size_t start_pos = _random_gen.random_number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    return comment;
  });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_parts_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * NUM_PARTS;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "P_PARTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<std::string>(table, "P_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_name_of_part(); });

  std::vector<std::string> manufacturers(table_size);
  std::generate(manufacturers.begin(), manufacturers.end(),
                [&]() { return std::to_string(_random_gen.random_number(1, 5)); });

  add_column<std::string>(table, "P_MFGR", cardinalities,
                          [&](std::vector<size_t> indices) { return "Manufacturer#" + manufacturers[indices[0]]; });

  add_column<std::string>(table, "P_BRAND", cardinalities, [&](std::vector<size_t> indices) {
    return "Brand#" + manufacturers[indices[0]] + std::to_string(_random_gen.random_number(1, 5));
  });

  add_column<std::string>(table, "P_TYPE", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_type_of_part(); });

  add_column<int>(table, "P_SIZE", cardinalities,
                  [&](std::vector<size_t> indices) { return _random_gen.random_number(1, 50); });

  add_column<std::string>(table, "P_CONTAINER", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_container_of_part(); });

  add_column<float>(table, "P_RETAILPRICE", cardinalities,
                    [&](std::vector<size_t> indices) { return calculate_part_retailprice(indices[0]); });

  add_column<std::string>(table, "P_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(5, 22); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_partsupps_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_scale_factor * NUM_PARTS, NUM_PARTSUPPS_PER_PART});

  add_column<int>(table, "PS_PARTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<int>(table, "PS_SUPPKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return calculate_partsuppkey(indices[0], indices[1]); });

  add_column<int>(table, "PS_AVAILQTY", cardinalities,
                  [&](std::vector<size_t>) { return _random_gen.random_number(1, 9999); });

  add_column<float>(table, "PS_SUPPLYCOST", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(100, 100000) / 100.f; });

  add_column<std::string>(table, "PS_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(49, 198); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_customers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * NUM_CUSTOMERS;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "C_CUSTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<std::string>(table, "C_NAME", cardinalities, [&](std::vector<size_t> indices) {
    return "Customer#" + _text_field_gen.pad_int_with_zeroes(indices[0], 9);
  });

  add_column<std::string>(table, "C_ADDRESS", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.v_string(10, 40); });

  std::vector<int> nationkeys(table_size);
  std::generate(nationkeys.begin(), nationkeys.end(), [&]() { return _random_gen.random_number(0, 24); });

  add_column<int>(table, "C_NATIONKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return nationkeys[indices[0]]; });

  add_column<std::string>(table, "C_PHONE", cardinalities, [&](std::vector<size_t> indices) {
    return _text_field_gen.generate_phone_number(nationkeys[indices[0]]);
  });

  add_column<float>(table, "C_ACCTBAL", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(-99999, 999999) / 100.f; });

  add_column<std::string>(table, "C_MKTSEGMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_customer_segment(); });

  add_column<std::string>(table, "C_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(29, 116); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

TableGenerator::order_lines_type TableGenerator::generate_order_lines() {
  const size_t total_orders_per_customer = NUM_ORDERS_PER_CUSTOMER * _scale_factor * NUM_CUSTOMERS;
  std::vector<std::vector<OrderLine>> all_order_lines(total_orders_per_customer);
  for (auto &order_lines_per_order : all_order_lines) {
    size_t order_lines_size = _random_gen.random_number(1, 7);
    order_lines_per_order.resize(order_lines_size);
    size_t order_i = &order_lines_per_order - &all_order_lines[0];
    // This key is only populated to 25%, according to the TPCH specification.
    // The first 8 of each 32 keys are used, the other 24 are unused.
    size_t orderkey = order_i / 8 * 32 + order_i % 8;
    size_t orderdate = _random_gen.random_number(_startdate, _enddate - 151 * _one_day);
    size_t linenumber = 0;
    for (auto &order_line : order_lines_per_order) {
      order_line.orderkey = orderkey;
      order_line.partkey = _random_gen.random_number(0, _scale_factor * NUM_PARTS - 1);
      order_line.linenumber = linenumber;
      order_line.quantity = _random_gen.random_number(1, 50);
      float part_retailprice = calculate_part_retailprice(order_line.partkey);
      order_line.extendedprice = order_line.quantity * part_retailprice;
      order_line.discount = _random_gen.random_number(1, 10) / 100.f;
      order_line.tax = _random_gen.random_number(1, 8) / 100.f;
      order_line.orderdate = orderdate;
      order_line.shipdate = orderdate + _random_gen.random_number(1, 121) * _one_day;
      order_line.receiptdate = order_line.shipdate + _random_gen.random_number(1, 30) * _one_day;
      order_line.linestatus = order_line.shipdate > _currentdate ? "O" : "F";
      linenumber++;
    }
  }
  return std::make_shared<std::vector<std::vector<OrderLine>>>(all_order_lines);
}

std::shared_ptr<opossum::Table> TableGenerator::generate_orders_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = NUM_ORDERS_PER_CUSTOMER * _scale_factor * NUM_CUSTOMERS;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "O_ORDERKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return order_lines->at(indices[0])[0].orderkey; });

  add_column<int>(table, "O_CUSTKEY", cardinalities, [&](std::vector<size_t>) {
    // O_CUSTKEY should be random between 0 and _scale_factor * NUM_CUSTOMERS,
    // but O_CUSTKEY % 3 must not be 0 (only two third of the keys are used).
    size_t max_compacted_key = _scale_factor * NUM_CUSTOMERS / 3 * 2 - 1;
    size_t compacted_key = _random_gen.random_number(0, max_compacted_key);
    return compacted_key / 2 * 3 + compacted_key % 2 + 1;
  });

  add_column<std::string>(table, "O_ORDERSTATUS", cardinalities, [&](std::vector<size_t> indices) {
    auto lines = order_lines->at(indices[0]);
    bool all_f = std::all_of(lines.begin(), lines.end(), [](OrderLine line) { return line.linestatus == "F"; });
    if (all_f) {
      return "F";
    }
    bool all_o = std::all_of(lines.begin(), lines.end(), [](OrderLine line) { return line.linestatus == "O"; });
    if (all_o) {
      return "O";
    }
    return "P";
  });

  add_column<float>(table, "O_TOTALPRICE", cardinalities, [&](std::vector<size_t> indices) {
    float sum = 0.f;
    for (auto &order_line : order_lines->at(indices[0])) {
      sum += order_line.extendedprice * (1 + order_line.tax) * (1 - order_line.discount);
    }
    return sum;
  });

  add_column<int>(table, "O_ORDERDATE", cardinalities,
                  [&](std::vector<size_t> indices) { return order_lines->at(indices[0])[0].orderdate; });

  add_column<std::string>(table, "O_ORDERPRIORITY", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_order_priority(); });

  add_column<std::string>(table, "O_CLERK", cardinalities, [&](std::vector<size_t>) {
    size_t clerk_number = _random_gen.random_number(1, 1000);
    return "Clerk#" + _text_field_gen.pad_int_with_zeroes(clerk_number, 9);
  });

  add_column<int>(table, "O_SHIPPRIORITY", cardinalities, [&](std::vector<size_t>) { return 0; });

  add_column<std::string>(table, "O_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(29, 116); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_lineitems_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = 0;
  std::vector<OrderLine> flattened_orderlines;
  for (const auto &order_lines_per_order : *order_lines) {
    table_size += order_lines_per_order.size();
    flattened_orderlines.insert(flattened_orderlines.end(), order_lines_per_order.begin(), order_lines_per_order.end());
  }
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "L_ORDERKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].orderkey; });

  add_column<int>(table, "L_PARTKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].partkey; });

  add_column<int>(table, "L_SUPPKEY", cardinalities, [&](std::vector<size_t> indices) {
    size_t supplier = _random_gen.random_number(0, 3);
    return calculate_partsuppkey(flattened_orderlines[indices[0]].partkey, supplier);
  });

  add_column<int>(table, "L_LINENUMBER", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].linenumber; });

  add_column<int>(table, "L_QUANTITY", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].quantity; });

  add_column<float>(table, "L_EXTENDEDPRICE", cardinalities,
                    [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].extendedprice; });

  add_column<float>(table, "L_DISCOUNT", cardinalities,
                    [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].discount; });

  add_column<float>(table, "L_TAX", cardinalities,
                    [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].tax; });

  add_column<std::string>(table, "L_RETURNFLAG", cardinalities, [&](std::vector<size_t> indices) {
    if (flattened_orderlines[indices[0]].receiptdate <= _currentdate) {
      if (_random_gen.random_number(0, 1)) {
        return "R";
      } else {
        return "A";
      }
    } else {
      return "N";
    }
  });

  add_column<std::string>(table, "L_LINESTATUS", cardinalities,
                          [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].linestatus; });

  add_column<int>(table, "L_SHIPDATE", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].shipdate; });

  add_column<int>(table, "L_COMMITDATE", cardinalities, [&](std::vector<size_t> indices) {
    auto orderdate = flattened_orderlines[indices[0]].orderdate;
    return orderdate + _random_gen.random_number(30, 90) * _one_day;
  });

  add_column<int>(table, "L_RECEIPTDATE", cardinalities,
                  [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].receiptdate; });

  add_column<std::string>(table, "L_SHIPINSTRUCT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_lineitem_instruction(); });

  add_column<std::string>(table, "L_SHIPMODE", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.generate_lineitem_mode(); });

  add_column<std::string>(table, "L_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(10, 43); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_nations_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_NATIONS});

  add_column<int>(table, "N_NATIONKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<std::string>(table, "N_NAME", cardinalities,
                          [&](std::vector<size_t> indices) { return _text_field_gen.nation_names[indices[0]]; });

  add_column<int>(table, "N_REGIONKEY", cardinalities,
                  [&](std::vector<size_t> indices) { return _region_keys_per_nation[indices[0]]; });

  add_column<std::string>(table, "N_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(31, 114); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_regions_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_REGIONS});

  // setup columns

  add_column<int>(table, "R_REGIONKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  add_column<std::string>(table, "R_NAME", cardinalities,
                          [&](std::vector<size_t> indices) { return _text_field_gen.region_names[indices[0]]; });

  add_column<std::string>(table, "R_COMMENT", cardinalities,
                          [&](std::vector<size_t>) { return _text_field_gen.text_string(31, 115); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::map<std::string, std::shared_ptr<opossum::Table>> TableGenerator::generate_all_tables() {
  auto supplier_table = generate_suppliers_table();
  auto parts_table = generate_parts_table();
  auto partsupps_table = generate_partsupps_table();
  auto customers_table = generate_customers_table();
  auto order_lines = generate_order_lines();
  auto orders_table = generate_orders_table(order_lines);
  auto lineitems_table = generate_lineitems_table(order_lines);
  auto nations_table = generate_nations_table();
  auto regions_table = generate_regions_table();

  return std::map<std::string, std::shared_ptr<opossum::Table>>({
          {"SUPPLIER", std::move(supplier_table)},
          {"PART", std::move(parts_table)},
          {"PARTSUPP", std::move(partsupps_table)},
          {"CUSTOMER", std::move(customers_table)},
          {"ORDERS", std::move(orders_table)},
          {"LINEITEM", std::move(lineitems_table)},
          {"NATION", std::move(nations_table)},
          {"REGION", std::move(regions_table)}});
}

}  // namespace tpch
