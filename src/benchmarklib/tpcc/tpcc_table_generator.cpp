#include "tpcc_table_generator.hpp"

#include <functional>
#include <future>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

TpccTableGenerator::TpccTableGenerator(const ChunkOffset chunk_size, const size_t warehouse_size,
                                       EncodingConfig encoding_config)
    : AbstractBenchmarkTableGenerator(chunk_size),
      _warehouse_size(warehouse_size),
      _encoding_config(std::move(encoding_config)) {}

std::shared_ptr<Table> TpccTableGenerator::generate_items_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_ITEMS});

  /**
   * indices[0] = item
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(columns_by_chunk, column_definitions, "I_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(columns_by_chunk, column_definitions, "I_IM_ID", cardinalities,
                  [&](std::vector<size_t>) { return _random_gen.random_number(1, 10000); });
  add_column<std::string>(columns_by_chunk, column_definitions, "I_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(14, 24); });
  add_column<float>(columns_by_chunk, column_definitions, "I_PRICE", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(100, 10000) / 100.f; });
  add_column<std::string>(
      columns_by_chunk, column_definitions, "I_DATA", cardinalities, [&](std::vector<size_t> indices) {
        std::string data = _random_gen.astring(26, 50);
        bool is_original = original_ids.find(indices[0]) != original_ids.end();
        if (is_original) {
          std::string original_string("ORIGINAL");
          size_t start_pos = _random_gen.random_number(0, data.length() - 1 - original_string.length());
          data.replace(start_pos, original_string.length(), original_string);
        }
        return data;
      });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("ITEM", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_warehouse_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size});

  /**
   * indices[0] = warehouse
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  add_column<int>(columns_by_chunk, column_definitions, "W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(columns_by_chunk, column_definitions, "W_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(columns_by_chunk, column_definitions, "W_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "W_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "W_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "W_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });

  add_column<std::string>(columns_by_chunk, column_definitions, "W_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(columns_by_chunk, column_definitions, "W_TAX", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  add_column<float>(columns_by_chunk, column_definitions, "W_YTD", cardinalities, [&](std::vector<size_t>) {
    return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT * NUM_DISTRICTS_PER_WAREHOUSE;
  });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("WAREHOUSE", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_stock_table() {
  auto cardinalities =
      std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size, NUM_STOCK_ITEMS});

  /**
   * indices[0] = warehouse
   * indices[1] = stock
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(columns_by_chunk, column_definitions, "S_I_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "S_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(columns_by_chunk, column_definitions, "S_QUANTITY", cardinalities,
                  [&](std::vector<size_t>) { return _random_gen.random_number(10, 100); });
  for (int district_i = 1; district_i <= 10; district_i++) {
    std::stringstream district_i_str;
    district_i_str << std::setw(2) << std::setfill('0') << district_i;
    add_column<std::string>(columns_by_chunk, column_definitions, "S_DIST_" + district_i_str.str(), cardinalities,
                            [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });
  }
  add_column<int>(columns_by_chunk, column_definitions, "S_YTD", cardinalities, [&](std::vector<size_t>) { return 0; });
  add_column<int>(columns_by_chunk, column_definitions, "S_ORDER_CNT", cardinalities,
                  [&](std::vector<size_t>) { return 0; });
  add_column<int>(columns_by_chunk, column_definitions, "S_REMOTE_CNT", cardinalities,
                  [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(
      columns_by_chunk, column_definitions, "S_DATA", cardinalities, [&](std::vector<size_t> indices) {
        std::string data = _random_gen.astring(26, 50);
        bool is_original = original_ids.find(indices[1]) != original_ids.end();
        if (is_original) {
          std::string original_string("ORIGINAL");
          size_t start_pos = _random_gen.random_number(0, data.length() - 1 - original_string.length());
          data.replace(start_pos, original_string.length(), original_string);
        }
        return data;
      });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("STOCK", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_district_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  add_column<int>(columns_by_chunk, column_definitions, "D_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "D_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(columns_by_chunk, column_definitions, "D_NAME", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(columns_by_chunk, column_definitions, "D_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "D_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "D_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "D_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });

  add_column<std::string>(columns_by_chunk, column_definitions, "D_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(columns_by_chunk, column_definitions, "D_TAX", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  add_column<float>(columns_by_chunk, column_definitions, "D_YTD", cardinalities,
                    [&](std::vector<size_t>) { return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT; });
  add_column<int>(columns_by_chunk, column_definitions, "D_NEXT_O_ID", cardinalities,
                  [&](std::vector<size_t>) { return NUM_ORDERS + 1; });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("DISTRICT", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_customer_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(columns_by_chunk, column_definitions, "C_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(columns_by_chunk, column_definitions, "C_D_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "C_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_FIRST", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(8, 16); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_MIDDLE", cardinalities,
                          [&](std::vector<size_t>) { return "OE"; });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_LAST", cardinalities,
                          [&](std::vector<size_t> indices) { return _random_gen.last_name(indices[2]); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_CITY", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_STATE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_PHONE", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.nstring(16, 16); });
  add_column<int>(columns_by_chunk, column_definitions, "C_SINCE", cardinalities,
                  [&](std::vector<size_t>) { return _current_date; });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_CREDIT", cardinalities,
                          [&](std::vector<size_t> indices) {
                            bool is_original = original_ids.find(indices[2]) != original_ids.end();
                            return is_original ? "BC" : "GC";
                          });
  add_column<int>(columns_by_chunk, column_definitions, "C_CREDIT_LIM", cardinalities,
                  [&](std::vector<size_t>) { return 50000; });
  add_column<float>(columns_by_chunk, column_definitions, "C_DISCOUNT", cardinalities,
                    [&](std::vector<size_t>) { return _random_gen.random_number(0, 5000) / 10000.f; });
  add_column<float>(columns_by_chunk, column_definitions, "C_BALANCE", cardinalities,
                    [&](std::vector<size_t>) { return -CUSTOMER_YTD; });
  add_column<float>(columns_by_chunk, column_definitions, "C_YTD_PAYMENT", cardinalities,
                    [&](std::vector<size_t>) { return CUSTOMER_YTD; });
  add_column<int>(columns_by_chunk, column_definitions, "C_PAYMENT_CNT", cardinalities,
                  [&](std::vector<size_t>) { return 1; });
  add_column<int>(columns_by_chunk, column_definitions, "C_DELIVERY_CNT", cardinalities,
                  [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(columns_by_chunk, column_definitions, "C_DATA", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(300, 500); });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("CUSTOMER", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_history_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{
      _warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT, NUM_HISTORY_ENTRIES});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   * indices[3] = history
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  add_column<int>(columns_by_chunk, column_definitions, "H_C_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(columns_by_chunk, column_definitions, "H_C_D_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "H_C_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(columns_by_chunk, column_definitions, "H_DATE", cardinalities,
                  [&](std::vector<size_t>) { return _current_date; });
  add_column<float>(columns_by_chunk, column_definitions, "H_AMOUNT", cardinalities,
                    [&](std::vector<size_t>) { return 10.f; });
  add_column<std::string>(columns_by_chunk, column_definitions, "H_DATA", cardinalities,
                          [&](std::vector<size_t>) { return _random_gen.astring(12, 24); });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("HISTORY", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_order_table(
    const TpccTableGenerator::order_line_counts_type& order_line_counts) {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  // TODO(anyone): generate a new customer permutation for each district and warehouse. Currently they all have the
  // same permutation
  auto customer_permutation = _random_gen.permutation(0, NUM_CUSTOMERS_PER_DISTRICT);

  add_column<int>(columns_by_chunk, column_definitions, "O_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[2]; });
  add_column<int>(columns_by_chunk, column_definitions, "O_D_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "O_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int>(columns_by_chunk, column_definitions, "O_C_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return customer_permutation[indices[2]]; });
  add_column<int>(columns_by_chunk, column_definitions, "O_ENTRY_D", cardinalities,
                  [&](std::vector<size_t>) { return _current_date; });
  // TODO(anybody) -1 should be null

  add_column<int>(columns_by_chunk, column_definitions, "O_CARRIER_ID", cardinalities,
                  [&](std::vector<size_t> indices) {
                    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _random_gen.random_number(1, 10) : -1;
                  });
  add_column<int>(columns_by_chunk, column_definitions, "O_OL_CNT", cardinalities,
                  [&](std::vector<size_t> indices) { return order_line_counts[indices[0]][indices[1]][indices[2]]; });
  add_column<int>(columns_by_chunk, column_definitions, "O_ALL_LOCAL", cardinalities,
                  [&](std::vector<size_t>) { return 1; });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("ORDER", table);
  return table;
}

TpccTableGenerator::order_line_counts_type TpccTableGenerator::generate_order_line_counts() {
  order_line_counts_type v(_warehouse_size);
  for (auto& v_per_warehouse : v) {
    v_per_warehouse.resize(NUM_DISTRICTS_PER_WAREHOUSE);
    for (auto& v_per_district : v_per_warehouse) {
      v_per_district.resize(NUM_ORDERS);
      for (auto& v_per_order : v_per_district) {
        v_per_order = _random_gen.random_number(5, 15);
      }
    }
  }
  return v;
}

/**
 * Generates a column for the 'ORDER_LINE' table. This is used in the specialization of add_column to insert vectors.
 * In contrast to other tables the ORDER_LINE table is NOT defined by saying, there are 10 order_lines per order,
 * but instead there 5 to 15 order_lines per order.
 * @tparam T
 * @param indices
 * @param order_line_counts
 * @param generator_function
 * @return
 */
template <typename T>
std::vector<T> TpccTableGenerator::_generate_inner_order_line_column(
    std::vector<size_t> indices, TpccTableGenerator::order_line_counts_type order_line_counts,
    const std::function<T(std::vector<size_t>)>& generator_function) {
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
void TpccTableGenerator::_add_order_line_column(std::vector<ChunkColumns>& columns_by_chunk,
                                                TableColumnDefinitions& column_definitions, std::string name,
                                                std::shared_ptr<std::vector<size_t>> cardinalities,
                                                TpccTableGenerator::order_line_counts_type order_line_counts,
                                                const std::function<T(std::vector<size_t>)>& generator_function) {
  const std::function<std::vector<T>(std::vector<size_t>)> wrapped_generator_function =
      [&](std::vector<size_t> indices) {
        return _generate_inner_order_line_column(indices, order_line_counts, generator_function);
      };
  add_column(columns_by_chunk, column_definitions, name, cardinalities, wrapped_generator_function);
}

std::shared_ptr<Table> TpccTableGenerator::generate_order_line_table(
    const TpccTableGenerator::order_line_counts_type& order_line_counts) {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   * indices[3] = order_line_size
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_O_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[2]; });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_D_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[1]; });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_W_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[0]; });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_NUMBER", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[3]; });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_I_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t>) { return _random_gen.random_number(1, NUM_ITEMS); });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_SUPPLY_W_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[0]; });
  // TODO(anybody) -1 should be null
  _add_order_line_column<int>(
      columns_by_chunk, column_definitions, "OL_DELIVERY_D", cardinalities, order_line_counts,
      [&](std::vector<size_t> indices) { return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _current_date : -1; });
  _add_order_line_column<int>(columns_by_chunk, column_definitions, "OL_QUANTITY", cardinalities, order_line_counts,
                              [&](std::vector<size_t>) { return 5; });

  _add_order_line_column<float>(
      columns_by_chunk, column_definitions, "OL_AMOUNT", cardinalities, order_line_counts,
      [&](std::vector<size_t> indices) {
        return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? 0.f : _random_gen.random_number(1, 999999) / 100.f;
      });
  _add_order_line_column<std::string>(columns_by_chunk, column_definitions, "OL_DIST_INFO", cardinalities,
                                      order_line_counts,
                                      [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("ORDER_LINE", table);
  return table;
}

std::shared_ptr<Table> TpccTableGenerator::generate_new_order_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = new_order
   */
  std::vector<ChunkColumns> columns_by_chunk;
  TableColumnDefinitions column_definitions;

  add_column<int>(columns_by_chunk, column_definitions, "NO_O_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[2] + NUM_ORDERS + 1 - NUM_NEW_ORDERS; });
  add_column<int>(columns_by_chunk, column_definitions, "NO_D_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[1]; });
  add_column<int>(columns_by_chunk, column_definitions, "NO_W_ID", cardinalities,
                  [&](std::vector<size_t> indices) { return indices[0]; });

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size, UseMvcc::Yes);
  for (const auto& chunk_columns : columns_by_chunk) table->append_chunk(chunk_columns);

  _encode_table("NEW_ORDER", table);
  return table;
}

std::map<std::string, std::shared_ptr<Table>> TpccTableGenerator::generate_all_tables() {
  std::vector<std::thread> threads;
  auto item_table = std::async(std::launch::async, &TpccTableGenerator::generate_items_table, this);
  auto warehouse_table = std::async(std::launch::async, &TpccTableGenerator::generate_warehouse_table, this);
  auto stock_table = std::async(std::launch::async, &TpccTableGenerator::generate_stock_table, this);
  auto district_table = std::async(std::launch::async, &TpccTableGenerator::generate_district_table, this);
  auto customer_table = std::async(std::launch::async, &TpccTableGenerator::generate_customer_table, this);
  auto history_table = std::async(std::launch::async, &TpccTableGenerator::generate_history_table, this);
  auto order_line_counts = std::async(std::launch::async, &TpccTableGenerator::generate_order_line_counts, this).get();
  auto order_table = std::async(std::launch::async, &TpccTableGenerator::generate_order_table, this, order_line_counts);
  auto order_line_table =
      std::async(std::launch::async, &TpccTableGenerator::generate_order_line_table, this, order_line_counts);
  auto new_order_table = std::async(std::launch::async, &TpccTableGenerator::generate_new_order_table, this);

  return std::map<std::string, std::shared_ptr<Table>>({{"ITEM", item_table.get()},
                                                        {"WAREHOUSE", warehouse_table.get()},
                                                        {"STOCK", stock_table.get()},
                                                        {"DISTRICT", district_table.get()},
                                                        {"CUSTOMER", customer_table.get()},
                                                        {"HISTORY", history_table.get()},
                                                        {"ORDER", order_table.get()},
                                                        {"ORDER_LINE", order_line_table.get()},
                                                        {"NEW_ORDER", new_order_table.get()}});
}

/*
 * This was introduced originally for the SQL REPL Console to be able to
 * a) generate a TPC-C table by table name (e.g. ITEM, WAREHOUSE), and
 * b) have all available table names browsable for the Console auto completion.
 */
TpccTableGeneratorFunctions TpccTableGenerator::table_generator_functions() {
  TpccTableGeneratorFunctions generators{
      {"ITEM", []() { return TpccTableGenerator().generate_items_table(); }},
      {"WAREHOUSE", []() { return TpccTableGenerator().generate_warehouse_table(); }},
      {"STOCK", []() { return TpccTableGenerator().generate_stock_table(); }},
      {"DISTRICT", []() { return TpccTableGenerator().generate_district_table(); }},
      {"CUSTOMER", []() { return TpccTableGenerator().generate_customer_table(); }},
      {"HISTORY", []() { return TpccTableGenerator().generate_history_table(); }},
      {"ORDER", []() { return TpccTableGenerator().generate_new_order_table(); }},
      {"NEW_ORDER",
       []() {
         auto order_line_counts = TpccTableGenerator().generate_order_line_counts();
         return TpccTableGenerator().generate_order_table(order_line_counts);
       }},
      {"ORDER_LINE", []() {
         auto order_line_counts = TpccTableGenerator().generate_order_line_counts();
         return TpccTableGenerator().generate_order_line_table(order_line_counts);
       }}};
  return generators;
}

std::shared_ptr<Table> TpccTableGenerator::generate_table(const std::string& table_name) {
  auto generators = TpccTableGenerator::table_generator_functions();
  if (generators.find(table_name) == generators.end()) {
    return nullptr;
  }
  return generators[table_name]();
}

void TpccTableGenerator::_encode_table(const std::string& table_name, const std::shared_ptr<Table>& table) {
  BenchmarkTableEncoder::encode(table_name, table, _encoding_config);
}

}  // namespace opossum
