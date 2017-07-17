#include "tpch_table_generator.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/value_column.hpp"

namespace tpch {

TableGenerator::TableGenerator() : _random_gen(RandomGenerator()), _text_field_gen(TextFieldGenerator(_random_gen)) {}

// TODO(anybody) chunk sizes and number of chunks might be tuned in generate_XYZ_table

template <typename T>
void TableGenerator::add_column(std::shared_ptr<opossum::Table> table, std::string name,
                                std::shared_ptr<std::vector<size_t>> cardinalities,
                                const std::function<T(std::vector<size_t>)> &generator_function) {
  /**
   * We have to add Chunks when we add the first column.
   * This has to be made after the first column was created and added,
   * because empty Chunks would be pruned right away.
   */
  bool is_first_column = table->col_count() == 0;

  auto data_type_name = opossum::name_of_type<T>();
  table->add_column_definition(name, data_type_name);

  /**
   * Calculate the total row count for this column based on the cardinalities of the influencing tables.
   * For the CUSTOMER table this calculates 1*10*3000
   */
  auto row_count = std::accumulate(std::begin(*cardinalities), std::end(*cardinalities), 1u, std::multiplies<size_t>());

  tbb::concurrent_vector<T> column;
  column.reserve(_chunk_size);

  /**
   * The loop over all records that the final column of the table will contain, e.g. row_count = 30 000 for CUSTOMER
   */
  for (size_t row_index = 0; row_index < row_count; row_index++) {
    std::vector<size_t> indices(cardinalities->size());

    /**
     * Calculate indices for internal loops
     *
     * We have to take care of writing IDs for referenced table correctly, e.g. when they are used as foreign key.
     * In that case the 'generator_function' has to be able to access the current index of our loops correctly,
     * which we ensure by defining them here.
     *
     * For example for CUSTOMER:
     * WAREHOUSE_ID | DISTRICT_ID | CUSTOMER_ID
     * indices[0]   | indices[1]  | indices[2]
     */
    for (size_t loop = 0; loop < cardinalities->size(); loop++) {
      auto divisor = std::accumulate(std::begin(*cardinalities) + loop + 1, std::end(*cardinalities), 1u,
                                     std::multiplies<size_t>());
      indices[loop] = (row_index / divisor) % cardinalities->at(loop);
    }

    /**
     * Actually generating and adding values.
     * Pass in the previously generated indices to use them in 'generator_function',
     * e.g. when generating IDs.
     */
    column.push_back(generator_function(indices));

    // write output chunks if column size has reached chunk_size
    if (row_index % _chunk_size == _chunk_size - 1) {
      auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));
      opossum::ChunkID chunk_id{static_cast<uint32_t>(row_index / _chunk_size)};

      // add Chunk if it is the first column, e.g. WAREHOUSE_ID in the example above
      if (is_first_column) {
        opossum::Chunk chunk(true);
        chunk.add_column(value_column);
        table->add_chunk(std::move(chunk));
      } else {
        auto &chunk = table->get_chunk(chunk_id);
        chunk.add_column(value_column);
      }

      // reset column
      column.clear();
      column.reserve(_chunk_size);
    }
  }

  // write partially filled last chunk
  if (row_count % _chunk_size != 0) {
    auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));

    // add Chunk if it is the first column, e.g. WAREHOUSE_ID in the example above
    if (is_first_column) {
      opossum::Chunk chunk(true);
      chunk.add_column(value_column);
      table->add_chunk(std::move(chunk));
    } else {
      opossum::ChunkID chunk_id{static_cast<uint32_t>(row_count / _chunk_size)};
      auto &chunk = table->get_chunk(chunk_id);
      chunk.add_column(value_column);
    }
  }
}

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

int TableGenerator::calculate_partsuppkey(size_t partkey, size_t supplier) const {
  size_t s = _scale_factor * _supplier_size;
  return (partkey + (supplier * (s / 4 + partkey / s))) % s;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_suppliers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * _supplier_size;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "S_SUPPKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "S_NAME", cardinalities, [&](std::vector<size_t> indices) { return "Supplier#" + _text_field_gen.fixed_length(indices[0], 9); });
  add_column<std::string>(table, "S_ADDRESS", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.v_string(10, 40); });
  std::vector<int> nationkeys (table_size);
  std::generate(nationkeys.begin(), nationkeys.end(), [&]() { return _random_gen.number(0, 24); });
  add_column<int>(table, "S_NATIONKEY", cardinalities, [&](std::vector<size_t> indices) { return nationkeys[indices[0]]; });
  add_column<std::string>(table, "S_PHONE", cardinalities, [&](std::vector<size_t> indices) { return _text_field_gen.phone_number(nationkeys[indices[0]]); });
  add_column<float>(table, "S_ACCTBAL", cardinalities, [&](std::vector<size_t>) { return _random_gen.number(-99999, 999999) / 100.f; });
  auto complaint_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);
  auto recommendation_ids = _random_gen.select_unique_ids(5 * _scale_factor, table_size);
  add_column<std::string>(table, "S_COMMENT", cardinalities, [&](std::vector<size_t> indices) {
    std::string comment = _text_field_gen.text_string(25, 100);
    bool complaints = complaint_ids.find(indices[0]) != complaint_ids.end();
    if (complaints) {
      std::string replacement("Customer Complaints");
      size_t start_pos = _random_gen.number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    bool recommends = recommendation_ids.find(indices[0]) != recommendation_ids.end();
    if (recommends) {
      std::string replacement("Customer Recommends");
      size_t start_pos = _random_gen.number(0, comment.length() - 1 - replacement.length());
      comment.replace(start_pos, replacement.length(), replacement);
    }
    return comment;
  });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_parts_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * _part_size;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "P_PARTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "P_NAME", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.part_name(); });
  std::vector<std::string> manufacturers(table_size);
  std::generate(manufacturers.begin(), manufacturers.end(), [&]() { return std::to_string(_random_gen.number(1, 5)); });
  add_column<std::string>(table, "P_MFGR", cardinalities, [&](std::vector<size_t> indices) {
    return "Manufacturer#" + manufacturers[indices[0]];
  });
  add_column<std::string>(table, "P_BRAND", cardinalities, [&](std::vector<size_t> indices) {
    return "Brand#" + manufacturers[indices[0]] + std::to_string(_random_gen.number(1, 5));
  });
  add_column<std::string>(table, "P_TYPE", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.part_type(); });
  add_column<int>(table, "P_SIZE", cardinalities, [&](std::vector<size_t> indices) { return _random_gen.number(1, 50); });
  add_column<std::string>(table, "P_CONTAINER", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.part_container(); });
  add_column<float>(table, "P_RETAILPRICE", cardinalities, [&](std::vector<size_t> indices) { return calculate_part_retailprice(indices[0]); });
  add_column<std::string>(table, "P_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(5, 22); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_partsupps_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_scale_factor * _part_size, _partsupp_size});

  add_column<int>(table, "PS_PARTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(table, "PS_SUPPKEY", cardinalities, [&](std::vector<size_t> indices) { return calculate_partsuppkey(indices[0], indices[1]); });
  add_column<int>(table, "PS_AVAILQTY", cardinalities, [&](std::vector<size_t>) { return _random_gen.number(1, 9999); });
  add_column<float>(table, "PS_SUPPLYCOST", cardinalities, [&](std::vector<size_t>) { return _random_gen.number(100, 100000) / 100.f; });
  add_column<std::string>(table, "PS_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(49, 198); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_customers_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _scale_factor * _customer_size;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "C_CUSTKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "C_NAME", cardinalities, [&](std::vector<size_t> indices) { return "Customer#" + _text_field_gen.fixed_length(indices[0], 9); });
  add_column<std::string>(table, "C_ADDRESS", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.v_string(10, 40); });
  std::vector<int> nationkeys (table_size);
  std::generate(nationkeys.begin(), nationkeys.end(), [&]() { return _random_gen.number(0, 24); });
  add_column<int>(table, "C_NATIONKEY", cardinalities, [&](std::vector<size_t> indices) { return nationkeys[indices[0]]; });
  add_column<std::string>(table, "C_PHONE", cardinalities, [&](std::vector<size_t> indices) { return _text_field_gen.phone_number(nationkeys[indices[0]]); });
  add_column<float>(table, "C_ACCTBAL", cardinalities, [&](std::vector<size_t>) { return _random_gen.number(-99999, 999999) / 100.f; });
  add_column<std::string>(table, "C_MKTSEGMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.customer_segment(); });
  add_column<std::string>(table, "C_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(29, 116); });

  opossum::DictionaryCompression::compress_table(*table);
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
    size_t linenumber = 0;
    for (auto &order_line : order_lines_per_order) {
      order_line.orderkey = orderkey;
      order_line.partkey = _random_gen.number(0, _scale_factor * _part_size - 1);
      order_line.linenumber = linenumber;
      order_line.quantity = _random_gen.number(1, 50);
      float part_retailprice = calculate_part_retailprice(order_line.partkey);
      order_line.extendedprice = order_line.quantity * part_retailprice;
      order_line.discount = _random_gen.number(1, 10) / 100.f;
      order_line.tax = _random_gen.number(1, 8) / 100.f;
      order_line.orderdate = orderdate;
      order_line.shipdate = orderdate + _random_gen.number(1, 121) * _one_day;
      order_line.receiptdate = order_line.shipdate + _random_gen.number(1, 30) * _one_day;
      order_line.linestatus = order_line.shipdate > _currentdate ? "O" : "F";
      linenumber++;
    }
  }
  return std::make_shared<std::vector<std::vector<OrderLine>>>(all_order_lines);
}

std::shared_ptr<opossum::Table> TableGenerator::generate_orders_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = _order_size * _scale_factor * _customer_size;
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "O_ORDERKEY", cardinalities, [&](std::vector<size_t> indices) { return order_lines->at(indices[0])[0].orderkey; });
  add_column<int>(table, "O_CUSTKEY", cardinalities, [&](std::vector<size_t>) {
    // O_CUSTKEY should be random between 0 and _scale_factor * _customer_size,
    // but O_CUSTKEY % 3 must not be 0 (only two third of the keys are used).
    size_t max_compacted_key = _scale_factor * _customer_size / 3 * 2 - 1;
    size_t compacted_key = _random_gen.number(0, max_compacted_key);
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
  add_column<int>(table, "O_ORDERDATE", cardinalities, [&](std::vector<size_t> indices) { return order_lines->at(indices[0])[0].orderdate; });
  add_column<std::string>(table, "O_ORDERPRIORITY", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.order_priority(); });
  add_column<std::string>(table, "O_CLERK", cardinalities, [&](std::vector<size_t>) {
    size_t clerk_number = _random_gen.number(1, 1000);
    return "Clerk#" + _text_field_gen.fixed_length(clerk_number, 9);
  });
  add_column<int>(table, "O_SHIPPRIORITY", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(table, "O_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(29, 116); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_lineitems_table(TableGenerator::order_lines_type order_lines) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  size_t table_size = 0;
  std::vector<OrderLine> flattened_orderlines;
  for(const auto &order_lines_per_order: *order_lines) {
    table_size += order_lines_per_order.size();
    flattened_orderlines.insert(flattened_orderlines.end(), order_lines_per_order.begin(), order_lines_per_order.end());
  }
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{table_size});

  add_column<int>(table, "L_ORDERKEY", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].orderkey; });
  add_column<int>(table, "L_PARTKEY", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].partkey; });
  add_column<int>(table, "L_SUPPKEY", cardinalities, [&](std::vector<size_t> indices) {
    size_t supplier = _random_gen.number(0, 3);
    return calculate_partsuppkey(flattened_orderlines[indices[0]].partkey, supplier);
  });
  add_column<int>(table, "L_LINENUMBER", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].linenumber; });
  add_column<int>(table, "L_QUANTITY", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].quantity; });
  add_column<float>(table, "L_EXTENDEDPRICE", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].extendedprice; });
  add_column<float>(table, "L_DISCOUNT", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].discount; });
  add_column<float>(table, "L_TAX", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].tax; });
  add_column<std::string>(table, "L_RETURNFLAG", cardinalities, [&](std::vector<size_t> indices) {
    if (flattened_orderlines[indices[0]].receiptdate <= _currentdate) {
      if (_random_gen.number(0, 1)) {
        return "R";
      } else {
        return "A";
      }
    } else {
      return "N";
    }
  });
  add_column<std::string>(table, "L_LINESTATUS", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].linestatus; });
  add_column<int>(table, "L_SHIPDATE", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].shipdate; });
  add_column<int>(table, "L_COMMITDATE", cardinalities, [&](std::vector<size_t> indices) {
    auto orderdate = flattened_orderlines[indices[0]].orderdate;
    return orderdate + _random_gen.number(30, 90) * _one_day;
  });
  add_column<int>(table, "L_RECEIPTDATE", cardinalities, [&](std::vector<size_t> indices) { return flattened_orderlines[indices[0]].receiptdate; });
  add_column<std::string>(table, "L_SHIPINSTRUCT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.lineitem_instruction(); });
  add_column<std::string>(table, "L_SHIPMODE", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.lineitem_mode(); });
  add_column<std::string>(table, "L_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(10, 43); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_nations_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_nation_size});

  add_column<int>(table, "N_NATIONKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "N_NAME", cardinalities, [&](std::vector<size_t> indices) { return _text_field_gen.nation_names[indices[0]]; });
  add_column<int>(table, "N_REGIONKEY", cardinalities, [&](std::vector<size_t> indices) { return _region_keys_per_nation[indices[0]]; });
  add_column<std::string>(table, "N_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(31, 114); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_regions_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_region_size});

  // setup columns
  add_column<int>(table, "R_REGIONKEY", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "R_NAME", cardinalities, [&](std::vector<size_t> indices) { return _text_field_gen.region_names[indices[0]]; });
  add_column<std::string>(table, "R_COMMENT", cardinalities, [&](std::vector<size_t>) { return _text_field_gen.text_string(31, 115); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<std::map<std::string, std::shared_ptr<opossum::Table>>> TableGenerator::generate_all_tables() {
  auto supplier_table = generate_suppliers_table();
  auto parts_table = generate_parts_table();
  auto partsupps_table = generate_partsupps_table();
  auto customers_table = generate_customers_table();
  auto order_lines = generate_order_lines();
  auto orders_table = generate_orders_table(order_lines);
  auto lineitems_table = generate_lineitems_table(order_lines);
  auto nations_table = generate_nations_table();
  auto regions_table = generate_regions_table();

  return std::make_shared<std::map<std::string, std::shared_ptr<opossum::Table>>>(
      std::initializer_list<std::map<std::string, std::shared_ptr<opossum::Table>>::value_type>{
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
