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
#include "storage/value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

TPCCTableGenerator::TPCCTableGenerator(int num_warehouses, const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config), _num_warehouses(num_warehouses) {}

TPCCTableGenerator::TPCCTableGenerator(int num_warehouses, uint32_t chunk_size)
    : AbstractTableGenerator(create_benchmark_config_with_chunk_size(chunk_size)), _num_warehouses(num_warehouses) {}

std::shared_ptr<Table> TPCCTableGenerator::generate_item_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_ITEMS});

  /**
   * indices[0] = item
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  _add_column<int32_t>(segments_by_chunk, column_definitions, "I_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "I_IM_ID", cardinalities,
                       [&](std::vector<size_t>) { return _random_gen.random_number(1, 10000); });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "I_NAME", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(14, 24)}; });
  _add_column<float>(segments_by_chunk, column_definitions, "I_PRICE", cardinalities,
                     [&](std::vector<size_t>) { return _random_gen.random_number(100, 10000) / 100.f; });
  _add_column<pmr_string>(
      segments_by_chunk, column_definitions, "I_DATA", cardinalities, [&](std::vector<size_t> indices) {
        std::string data = _random_gen.astring(26, 50);
        bool is_original = original_ids.find(indices[0]) != original_ids.end();
        if (is_original) {
          std::string original_string("ORIGINAL");
          size_t start_pos = _random_gen.random_number(0, data.length() - 1 - original_string.length());
          data.replace(start_pos, original_string.length(), original_string);
        }
        return pmr_string{data};
      });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_warehouse_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_num_warehouses});

  /**
   * indices[0] = warehouse
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_column<int32_t>(segments_by_chunk, column_definitions, "W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_NAME", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(6, 10)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_CITY", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_STATE", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(2, 2)}; });

  _add_column<pmr_string>(segments_by_chunk, column_definitions, "W_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.zip_code()}; });
  _add_column<float>(segments_by_chunk, column_definitions, "W_TAX", cardinalities,
                     [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  _add_column<float>(segments_by_chunk, column_definitions, "W_YTD", cardinalities, [&](std::vector<size_t>) {
    return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT * NUM_DISTRICTS_PER_WAREHOUSE;
  });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_stock_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_STOCK_ITEMS_PER_WAREHOUSE});

  /**
   * indices[0] = warehouse
   * indices[1] = stock
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_I_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_QUANTITY", cardinalities,
                       [&](std::vector<size_t>) { return _random_gen.random_number(10, 100); });
  for (int district_i = 1; district_i <= 10; district_i++) {
    std::stringstream district_i_str;
    district_i_str << std::setw(2) << std::setfill('0') << district_i;
    _add_column<pmr_string>(segments_by_chunk, column_definitions, "S_DIST_" + district_i_str.str(), cardinalities,
                            [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(24, 24)}; });
  }
  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_YTD", cardinalities,
                       [&](std::vector<size_t>) { return 0; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_ORDER_CNT", cardinalities,
                       [&](std::vector<size_t>) { return 0; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "S_REMOTE_CNT", cardinalities,
                       [&](std::vector<size_t>) { return 0; });
  _add_column<pmr_string>(
      segments_by_chunk, column_definitions, "S_DATA", cardinalities, [&](std::vector<size_t> indices) {
        std::string data = _random_gen.astring(26, 50);
        bool is_original = original_ids.find(indices[1]) != original_ids.end();
        if (is_original) {
          std::string original_string("ORIGINAL");
          size_t start_pos = _random_gen.random_number(0, data.length() - 1 - original_string.length());
          data.replace(start_pos, original_string.length(), original_string);
        }
        return pmr_string{data};
      });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_district_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_column<int32_t>(segments_by_chunk, column_definitions, "D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "D_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_NAME", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(6, 10)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_CITY", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_STATE", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(2, 2)}; });

  _add_column<pmr_string>(segments_by_chunk, column_definitions, "D_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.zip_code()}; });
  _add_column<float>(segments_by_chunk, column_definitions, "D_TAX", cardinalities,
                     [&](std::vector<size_t>) { return _random_gen.random_number(0, 2000) / 10000.f; });
  _add_column<float>(segments_by_chunk, column_definitions, "D_YTD", cardinalities,
                     [&](std::vector<size_t>) { return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "D_NEXT_O_ID", cardinalities,
                       [&](std::vector<size_t>) { return NUM_ORDERS_PER_DISTRICT + 1; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_customer_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[2] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_FIRST", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(8, 16)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_MIDDLE", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{"OE"}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_LAST", cardinalities,
                          [&](std::vector<size_t> indices) { return pmr_string{_random_gen.last_name(indices[2])}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_STREET_1", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_STREET_2", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_CITY", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(10, 20)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_STATE", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(2, 2)}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_ZIP", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.zip_code()}; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_PHONE", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.nstring(16, 16)}; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_SINCE", cardinalities,
                       [&](std::vector<size_t>) { return _current_date; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_CREDIT", cardinalities,
                          [&](std::vector<size_t> indices) {
                            bool is_original = original_ids.find(indices[2]) != original_ids.end();
                            return pmr_string{is_original ? "BC" : "GC"};
                          });
  _add_column<float>(segments_by_chunk, column_definitions, "C_CREDIT_LIM", cardinalities,
                     [&](std::vector<size_t>) { return 50000; });
  _add_column<float>(segments_by_chunk, column_definitions, "C_DISCOUNT", cardinalities,
                     [&](std::vector<size_t>) { return _random_gen.random_number(0, 5000) / 10000.f; });
  _add_column<float>(segments_by_chunk, column_definitions, "C_BALANCE", cardinalities,
                     [&](std::vector<size_t>) { return -CUSTOMER_YTD; });
  _add_column<float>(segments_by_chunk, column_definitions, "C_YTD_PAYMENT", cardinalities,
                     [&](std::vector<size_t>) { return CUSTOMER_YTD; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_PAYMENT_CNT", cardinalities,
                       [&](std::vector<size_t>) { return 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "C_DELIVERY_CNT", cardinalities,
                       [&](std::vector<size_t>) { return 0; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "C_DATA", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(300, 500)}; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  _random_gen.reset_c_for_c_last();

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_history_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{
      _num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT, NUM_HISTORY_ENTRIES_PER_CUSTOMER});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   * indices[3] = history
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_C_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[2] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_C_D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_C_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "H_DATE", cardinalities,
                       [&](std::vector<size_t>) { return _current_date; });
  _add_column<float>(segments_by_chunk, column_definitions, "H_AMOUNT", cardinalities,
                     [&](std::vector<size_t>) { return 10.f; });
  _add_column<pmr_string>(segments_by_chunk, column_definitions, "H_DATA", cardinalities,
                          [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(12, 24)}; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_order_table(
    const TPCCTableGenerator::OrderLineCounts& order_line_counts) {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  // TODO(anyone): generate a new customer permutation for each district and warehouse. Currently they all have the
  // same permutation
  auto customer_permutation = _random_gen.permutation(0, NUM_CUSTOMERS_PER_DISTRICT);

  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[2] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_C_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return customer_permutation[indices[2]] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_ENTRY_D", cardinalities,
                       [&](std::vector<size_t>) { return _current_date; });
  // TODO(anybody) -1 should be null

  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_CARRIER_ID", cardinalities,
                       [&](std::vector<size_t> indices) {
                         return indices[2] + 1 <= NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT
                                    ? _random_gen.random_number(1, 10)
                                    : -1;
                       });
  _add_column<int32_t>(
      segments_by_chunk, column_definitions, "O_OL_CNT", cardinalities,
      [&](std::vector<size_t> indices) { return order_line_counts[indices[0]][indices[1]][indices[2]]; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "O_ALL_LOCAL", cardinalities,
                       [&](std::vector<size_t>) { return 1; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

TPCCTableGenerator::OrderLineCounts TPCCTableGenerator::generate_order_line_counts() {
  OrderLineCounts v(_num_warehouses);
  for (auto& v_per_warehouse : v) {
    v_per_warehouse.resize(NUM_DISTRICTS_PER_WAREHOUSE);
    for (auto& v_per_district : v_per_warehouse) {
      v_per_district.resize(NUM_ORDERS_PER_DISTRICT);
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
std::vector<T> TPCCTableGenerator::_generate_inner_order_line_column(
    std::vector<size_t> indices, TPCCTableGenerator::OrderLineCounts order_line_counts,
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
void TPCCTableGenerator::_add_order_line_column(std::vector<Segments>& segments_by_chunk,
                                                TableColumnDefinitions& column_definitions, std::string name,
                                                std::shared_ptr<std::vector<size_t>> cardinalities,
                                                TPCCTableGenerator::OrderLineCounts order_line_counts,
                                                const std::function<T(std::vector<size_t>)>& generator_function) {
  const std::function<std::vector<T>(std::vector<size_t>)> wrapped_generator_function =
      [&](std::vector<size_t> indices) {
        return _generate_inner_order_line_column(indices, order_line_counts, generator_function);
      };
  _add_column(segments_by_chunk, column_definitions, name, cardinalities, wrapped_generator_function);
}

std::shared_ptr<Table> TPCCTableGenerator::generate_order_line_table(
    const TPCCTableGenerator::OrderLineCounts& order_line_counts) {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   * indices[3] = order_line_size
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_O_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[2] + 1; });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_D_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_W_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[0] + 1; });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_NUMBER", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[3] + 1; });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_I_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t>) { return _random_gen.random_number(1, NUM_ITEMS); });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_SUPPLY_W_ID", cardinalities, order_line_counts,
                              [&](std::vector<size_t> indices) { return indices[0] + 1; });
  // TODO(anybody) -1 should be null
  _add_order_line_column<int>(
      segments_by_chunk, column_definitions, "OL_DELIVERY_D", cardinalities, order_line_counts,
      [&](std::vector<size_t> indices) {
        return indices[2] + 1 <= NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT ? _current_date : -1;
      });
  _add_order_line_column<int>(segments_by_chunk, column_definitions, "OL_QUANTITY", cardinalities, order_line_counts,
                              [&](std::vector<size_t>) { return 5; });

  _add_order_line_column<float>(segments_by_chunk, column_definitions, "OL_AMOUNT", cardinalities, order_line_counts,
                                [&](std::vector<size_t> indices) {
                                  return indices[2] < NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT
                                             ? 0.f
                                             : _random_gen.random_number(1, 999999) / 100.f;
                                });
  _add_order_line_column<pmr_string>(segments_by_chunk, column_definitions, "OL_DIST_INFO", cardinalities,
                                     order_line_counts,
                                     [&](std::vector<size_t>) { return pmr_string{_random_gen.astring(24, 24)}; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_new_order_table() {
  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_num_warehouses, NUM_DISTRICTS_PER_WAREHOUSE, NUM_NEW_ORDERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = new_order
   */
  std::vector<Segments> segments_by_chunk;
  TableColumnDefinitions column_definitions;

  _add_column<int32_t>(segments_by_chunk, column_definitions, "NO_O_ID", cardinalities,
                       [&](std::vector<size_t> indices) {
                         return indices[2] + 1 + NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT;
                       });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "NO_D_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[1] + 1; });
  _add_column<int32_t>(segments_by_chunk, column_definitions, "NO_W_ID", cardinalities,
                       [&](std::vector<size_t> indices) { return indices[0] + 1; });

  auto table =
      std::make_shared<Table>(column_definitions, TableType::Data, _benchmark_config->chunk_size, UseMvcc::Yes);
  for (const auto& segments : segments_by_chunk) {
    const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    table->append_chunk(segments, mvcc_data);
  }

  return table;
}

std::unordered_map<std::string, BenchmarkTableInfo> TPCCTableGenerator::generate() {
  Assert(!_benchmark_config->cache_binary_tables, "Caching binary tables is not yet supported for TPC-C");

  std::vector<std::thread> threads;
  auto item_table = generate_item_table();
  auto warehouse_table = generate_warehouse_table();
  auto stock_table = generate_stock_table();
  auto district_table = generate_district_table();
  auto customer_table = generate_customer_table();
  auto history_table = generate_history_table();
  auto new_order_table = generate_new_order_table();

  auto order_line_counts = generate_order_line_counts();
  auto order_table = generate_order_table(order_line_counts);
  auto order_line_table = generate_order_line_table(order_line_counts);

  return std::unordered_map<std::string, BenchmarkTableInfo>({{"ITEM", BenchmarkTableInfo{item_table}},
                                                              {"WAREHOUSE", BenchmarkTableInfo{warehouse_table}},
                                                              {"STOCK", BenchmarkTableInfo{stock_table}},
                                                              {"DISTRICT", BenchmarkTableInfo{district_table}},
                                                              {"CUSTOMER", BenchmarkTableInfo{customer_table}},
                                                              {"HISTORY", BenchmarkTableInfo{history_table}},
                                                              {"ORDER", BenchmarkTableInfo{order_table}},
                                                              {"ORDER_LINE", BenchmarkTableInfo{order_line_table}},
                                                              {"NEW_ORDER", BenchmarkTableInfo{new_order_table}}});
}

thread_local TPCCRandomGenerator TPCCTableGenerator::_random_gen;  // NOLINT

}  // namespace opossum
