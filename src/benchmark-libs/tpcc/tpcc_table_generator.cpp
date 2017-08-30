#include "tpcc_table_generator.hpp"

#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constants.hpp"

#include "storage/chunk.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace tpcc {

TpccTableGenerator::TpccTableGenerator(const opossum::ChunkOffset chunk_size, const size_t warehouse_size)
    : AbstractBenchmarkTableGenerator(chunk_size),
      _warehouse_size(warehouse_size),
      _random_gen(TpccRandomGenerator()) {}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_items_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_ITEMS});

  /**
   * indices[0] = item
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(table, "I_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(table, "I_IM_ID", cardinalities,
                  [&](std::vector<size_t>) { return _random_gen.random_number(1, 10000); });
  add_column<std::string>(table, "I_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(14, 24); });
  add_column<float>(table, "I_PRICE", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(100, 10000) / 100.f; });
  add_column<std::string>(table, "I_DATA", cardinalities, [&](std::vector<size_t> indices) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(indices[0]) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.random_number(0, data.length() - 1 - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  });

  opossum::DictionaryCompression::compress_table(*table);

  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_warehouse_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size});

  /**
   * indices[0] = warehouse
   */

  add_column<int>(table, "W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "W_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(table, "W_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "W_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "W_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "W_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });

  add_column<std::string>(table, "W_ZIP", cardinalities, [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(table, "W_TAX", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  add_column<float>(table, "W_YTD", cardinalities, [&](std::vector<size_t>) {
    return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT * NUM_DISTRICTS_PER_WAREHOUSE;
  });

  opossum::DictionaryCompression::compress_table(*table);

  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_stock_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities =
      std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size, NUM_STOCK_ITEMS});

  /**
   * indices[0] = warehouse
   * indices[1] = stock
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(table, "S_I_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "S_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(table, "S_QUANTITY", cardinalities,
                  [&](std::vector<size_t>) { return _random_gen.random_number(10, 100); });
  for (int district_i = 1; district_i <= 10; district_i++) {
    std::stringstream district_i_str;
    district_i_str << std::setw(2) << std::setfill('0') << district_i;
    add_column<std::string>(table, "S_DIST_" + district_i_str.str(), cardinalities,
                            [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });
  }
  add_column<int>(table, "S_YTD", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<int>(table, "S_ORDER_CNT", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<int>(table, "S_REMOTE_CNT", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(table, "S_DATA", cardinalities, [&](std::vector<size_t> indices) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(indices[1]) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.random_number(0, data.length() - 1 - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_district_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   */

  add_column<int>(table, "D_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "D_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "D_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(table, "D_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "D_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "D_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "D_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });

  add_column<std::string>(table, "D_ZIP", cardinalities, [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(table, "D_TAX", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  add_column<float>(table, "D_YTD", cardinalities,
                    [&](std::vector<size_t>) { return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT; });
  add_column<int>(table, "D_NEXT_O_ID", cardinalities, [&](std::vector<size_t>) { return NUM_ORDERS + 1; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_customer_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(table, "C_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(table, "C_D_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "C_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(table, "C_FIRST", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(8, 16); });
  add_column<std::string>(table, "C_MIDDLE", cardinalities, [&](std::vector<size_t>) { return "OE"; });
  add_column<std::string>(table, "C_LAST", cardinalities,
                          [&](std::vector<size_t> indices) { return _random_gen.last_name(indices[2]); });
  add_column<std::string>(table, "C_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "C_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "C_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(table, "C_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });
  add_column<std::string>(table, "C_ZIP", cardinalities, [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<std::string>(table, "C_PHONE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.nstring(16, 16); });
  add_column<int>(table, "C_SINCE", cardinalities, [&](std::vector<size_t>) { return _current_date; });
  add_column<std::string>(table, "C_CREDIT", cardinalities, [&](std::vector<size_t> indices) {
    bool is_original = original_ids.find(indices[2]) != original_ids.end();
    return is_original ? "BC" : "GC";
  });
  add_column<int>(table, "C_CREDIT_LIM", cardinalities, [&](std::vector<size_t>) { return 50000; });
  add_column<float>(table, "C_DISCOUNT", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 5000) / 10000.f; });
  add_column<float>(table, "C_BALANCE", cardinalities, [&](std::vector<size_t>) { return -CUSTOMER_YTD; });
  add_column<float>(table, "C_YTD_PAYMENT", cardinalities, [&](std::vector<size_t>) { return CUSTOMER_YTD; });
  add_column<int>(table, "C_PAYMENT_CNT", cardinalities, [&](std::vector<size_t>) { return 1; });
  add_column<int>(table, "C_DELIVERY_CNT", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(table, "C_DATA", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(300, 500); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_history_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{
      _warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT, NUM_HISTORY_ENTRIES});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   * indices[3] = history
   */

  add_column<int>(table, "H_C_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(table, "H_C_D_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "H_C_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(table, "H_DATE", cardinalities, [&](std::vector<size_t>) { return _current_date; });
  add_column<float>(table, "H_AMOUNT", cardinalities, [&](std::vector<size_t>) { return 10.f; });
  add_column<std::string>(table, "H_DATA", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(12, 24); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_order_table(
    TpccTableGenerator::order_line_counts_type order_line_counts) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   */

  // TODO(anyone): generate a new customer permutation for each district and warehouse. Currently they all have the
  // same permutation
  auto customer_permutation = _random_gen.permutation(0, NUM_CUSTOMERS_PER_DISTRICT);

  add_column<int>(table, "O_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(table, "O_D_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "O_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(table, "O_C_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return customer_permutation[indices[2]]; });
  add_column<int>(table, "O_ENTRY_D", cardinalities, [&](std::vector<size_t>) { return _current_date; });
  // TODO(anybody) -1 should be null

  add_column<int>(table, "O_CARRIER_ID", cardinalities, [&](std::vector<size_t> indices) {
    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _random_gen.random_number(1, 10) : -1;
  });
  add_column<int>(table, "O_OL_CNT", cardinalities,
                  [&](std::vector<size_t> indices) { return order_line_counts[indices[0]][indices[1]][indices[2]]; });
  add_column<int>(table, "O_ALL_LOCAL", cardinalities, [&](std::vector<size_t>) { return 1; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

TpccTableGenerator::order_line_counts_type TpccTableGenerator::generate_order_line_counts() {
  order_line_counts_type v(_warehouse_size);
  for (auto &v_per_warehouse : v) {
    v_per_warehouse.resize(NUM_DISTRICTS_PER_WAREHOUSE);
    for (auto &v_per_district : v_per_warehouse) {
      v_per_district.resize(NUM_ORDERS);
      for (auto &v_per_order : v_per_district) {
        v_per_order = _random_gen.random_number(5, 15);
      }
    }
  }
  return v;
}

/**
 * Generates a column for the 'ORDER-LINE' table. This is used in the specialization of add_column to insert vectors.
 * In contrast to other tables the ORDER-LINE table is NOT defined by saying, there are 10 order-line per order,
 * but instead there 5 to 15 order-lines per order.
 * @tparam T
 * @param indices
 * @param order_line_counts
 * @param generator_function
 * @return
 */
template <typename T>
std::vector<T> TpccTableGenerator::generate_inner_order_line_column(
    std::vector<size_t> indices, TpccTableGenerator::order_line_counts_type order_line_counts,
    const std::function<T(std::vector<size_t>)> &generator_function) {
  auto order_line_count = order_line_counts[indices[0]][indices[1]][indices[2]];

  std::vector<T> values;
  values.reserve(order_line_count);
  for (size_t i = 0; i < order_line_count; i++) {
    auto copied_indices = indices;
    copied_indices.push_back(i);
    values.push_back(generator_function(copied_indices));
  }

  return values;
}

template <typename T>
void TpccTableGenerator::add_order_line_column(std::shared_ptr<opossum::Table> table, std::string name,
                                               std::shared_ptr<std::vector<size_t>> cardinalities,
                                               TpccTableGenerator::order_line_counts_type order_line_counts,
                                               const std::function<T(std::vector<size_t>)> &generator_function) {
  const std::function<std::vector<T>(std::vector<size_t>)> wrapped_generator_function =
      [&](std::vector<size_t> indices) {
        return generate_inner_order_line_column(indices, order_line_counts, generator_function);
      };
  add_column(table, name, cardinalities, wrapped_generator_function);
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_order_line_table(
    TpccTableGenerator::order_line_counts_type order_line_counts) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   * indices[3] = order_line_size
   */

  add_order_line_column<int>(table, "OL_O_ID", cardinalities, order_line_counts,
                             [&](std::vector<size_t> indices) { return indices[2]; });
  add_order_line_column<int>(table, "OL_D_ID", cardinalities, order_line_counts,
                             [&](std::vector<size_t> indices) { return indices[1]; });
  add_order_line_column<int>(table, "OL_W_ID", cardinalities, order_line_counts,
                             [&](std::vector<size_t> indices) { return indices[0]; });
  add_order_line_column<int>(table, "OL_NUMBER", cardinalities, order_line_counts,
                             [&](std::vector<size_t> indices) { return indices[3]; });
  add_order_line_column<int>(table, "OL_I_ID", cardinalities, order_line_counts,
                             [&](std::vector<size_t>) { return _random_gen.random_number(1, NUM_ITEMS); });
  add_order_line_column<int>(table, "OL_SUPPLY_W_ID", cardinalities, order_line_counts,
                             [&](std::vector<size_t> indices) { return indices[0]; });
  // TODO(anybody) -1 should be null
  add_order_line_column<int>(
      table, "OL_DELIVERY_D", cardinalities, order_line_counts,
      [&](std::vector<size_t> indices) { return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _current_date : -1; });
  add_order_line_column<int>(table, "OL_QUANTITY", cardinalities, order_line_counts,
                             [&](std::vector<size_t>) { return 5; });

  add_order_line_column<float>(table, "OL_AMOUNT", cardinalities, order_line_counts, [&](std::vector<size_t> indices) {
    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? 0.f : _random_gen.random_number(1, 999999) / 100.f;
  });
  add_order_line_column<std::string>(table, "OL_DIST_INFO", cardinalities, order_line_counts,
                                     [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_new_order_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = new_order
   */
  add_column<int>(table, "NO_O_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[2] + NUM_ORDERS + 1 - NUM_NEW_ORDERS; });
  add_column<int>(table, "NO_D_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(table, "NO_W_ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::map<std::string, std::shared_ptr<opossum::Table>> TpccTableGenerator::generate_all_tables() {
  auto item_table = generate_items_table();
  auto warehouse_table = generate_warehouse_table();
  auto stock_table = generate_stock_table();
  auto district_table = generate_district_table();
  auto customer_table = generate_customer_table();
  auto history_table = generate_history_table();
  auto order_line_counts = generate_order_line_counts();
  auto order_table = generate_order_table(order_line_counts);
  auto order_line_table = generate_order_line_table(order_line_counts);
  auto new_order_table = generate_new_order_table();

  return std::map<std::string, std::shared_ptr<opossum::Table>>({{"ITEM", std::move(item_table)},
                                                                 {"WAREHOUSE", std::move(warehouse_table)},
                                                                 {"STOCK", std::move(stock_table)},
                                                                 {"DISTRICT", std::move(district_table)},
                                                                 {"CUSTOMER", std::move(customer_table)},
                                                                 {"HISTORY", std::move(history_table)},
                                                                 {"ORDER", std::move(order_table)},
                                                                 {"ORDER-LINE", std::move(order_line_table)},
                                                                 {"NEW-ORDER", std::move(new_order_table)}});
}

/*
 * This was introduced originally for the SQL REPL Console to be able to
 * a) generate a TPC-C table by table name (e.g. ITEM, WAREHOUSE), and
 * b) have all available table names browsable for the Console auto completion.
 */
TpccTableGeneratorFunctions TpccTableGenerator::tpcc_table_generator_functions() {
  TpccTableGeneratorFunctions generators{
      {"ITEM", []() { return tpcc::TpccTableGenerator().generate_items_table(); }},
      {"WAREHOUSE", []() { return tpcc::TpccTableGenerator().generate_warehouse_table(); }},
      {"STOCK", []() { return tpcc::TpccTableGenerator().generate_stock_table(); }},
      {"DISTRICT", []() { return tpcc::TpccTableGenerator().generate_district_table(); }},
      {"CUSTOMER", []() { return tpcc::TpccTableGenerator().generate_customer_table(); }},
      {"HISTORY", []() { return tpcc::TpccTableGenerator().generate_history_table(); }},
      {"ORDER", []() { return tpcc::TpccTableGenerator().generate_new_order_table(); }},
      {"NEW-ORDER",
       []() {
         auto order_line_counts = tpcc::TpccTableGenerator().generate_order_line_counts();
         return tpcc::TpccTableGenerator().generate_order_table(order_line_counts);
       }},
      {"ORDER-LINE", []() {
         auto order_line_counts = tpcc::TpccTableGenerator().generate_order_line_counts();
         return tpcc::TpccTableGenerator().generate_order_line_table(order_line_counts);
       }}};
  return generators;
}

std::shared_ptr<opossum::Table> TpccTableGenerator::generate_tpcc_table(const std::string &tablename) {
  auto generators = TpccTableGenerator::tpcc_table_generator_functions();
  if (generators.find(tablename) == generators.end()) {
    return nullptr;
  }
  return generators[tablename]();
}

}  // namespace tpcc
