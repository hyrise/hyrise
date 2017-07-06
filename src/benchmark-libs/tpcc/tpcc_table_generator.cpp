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
#include "resolve_type.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/value_column.hpp"

namespace tpcc {

TableGenerator::TableGenerator() : _random_gen(RandomGenerator()) {}

template <typename T>
tbb::concurrent_vector<T> TableGenerator::generate_order_line_column(
    std::vector<size_t> indices, TableGenerator::order_line_counts_type order_line_counts,
    const std::function<T(std::vector<size_t>)> &generator_function) {
  auto order_line_count = order_line_counts[indices[0]][indices[1]][indices[2]];

  tbb::concurrent_vector<T> values;
  values.reserve(order_line_count);
  for (size_t i = 0; i < order_line_count; i++) {
    auto copied_indices = indices;
    copied_indices.push_back(i);
    values.push_back(generator_function(copied_indices));
  }

  return values;
}

/**
 * Specialized version for `ORDER-LINE` table.
 * TODO(anyone): look into how to merge this.
 */
template <typename T>
void TableGenerator::add_column(std::shared_ptr<std::vector<size_t>> cardinalities,
                                TableGenerator::order_line_counts_type order_line_counts,
                                std::shared_ptr<opossum::Table> table, std::string name,
                                const std::function<T(std::vector<size_t>)> &generator_function) {
  /**
   * We have to add Chunks when we add the first column.
   * This has to be made after the first column was created and added,
   * because empty Chunks would be pruned right away.
   */
  bool is_first_column = table->col_count() == 0;

  auto data_type_name = opossum::name_of_type<T>();
  table->add_column_definition(name, data_type_name);

  auto row_count = std::accumulate(std::begin(*cardinalities), std::end(*cardinalities), 1u, std::multiplies<size_t>());

  tbb::concurrent_vector<T> column;
  column.reserve(_chunk_size);

  size_t row_index = 0;

  for (size_t loop_index = 0; loop_index < row_count; loop_index++) {
    std::vector<size_t> indices(cardinalities->size());

    // calculate indices for internal loops
    for (size_t loop = 0; loop < cardinalities->size(); loop++) {
      auto divisor = std::accumulate(std::begin(*cardinalities) + loop + 1, std::end(*cardinalities), 1u,
                                     std::multiplies<size_t>());
      indices[loop] = (loop_index / divisor) % cardinalities->at(loop);
    }

    // actually generating and adding values
    auto values = generate_order_line_column(indices, order_line_counts, generator_function);
    for (T &value : values) {
      column.push_back(value);

      // write output chunks if column size has reached chunk_size
      if (row_index % _chunk_size == _chunk_size - 1) {
        auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));

        if (is_first_column) {
          opossum::Chunk chunk(true);
          chunk.add_column(value_column);
          table->add_chunk(std::move(chunk));
        } else {
          opossum::ChunkID chunk_id{static_cast<uint32_t>(row_index / _chunk_size)};
          auto &chunk = table->get_chunk(chunk_id);
          chunk.add_column(value_column);
        }

        // reset column
        column.clear();
        column.reserve(_chunk_size);
      }
      row_index++;
    }
  }

  // write partially filled last chunk
  if (row_index % _chunk_size != 0) {
    auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));

    if (is_first_column) {
      opossum::Chunk chunk(true);
      chunk.add_column(value_column);
      table->add_chunk(std::move(chunk));
    } else {
      opossum::ChunkID chunk_id{static_cast<uint32_t>(row_index / _chunk_size)};
      auto &chunk = table->get_chunk(chunk_id);
      chunk.add_column(value_column);
    }
  }
}

/**
 * In TPCC table sizes are usually defined relatively to each other.
 * E.g. the specification defines that there are 10 district for each warehouse.
 *
 * A trivial approach to implement this in our table generator would be to iterate in nested loops and add all rows.
 * However, this makes it hard to take care of a certain chunk_size. With nested loops
 * chunks only contain as many rows as there are iterations in the most inner loop.
 *
 * In this method we basically generate the whole column in a single loop,
 * so that we can easily split when a Chunk is full.
 *
 * @tparam T                  the type of the column
 * @param cardinalities       the cardinalities of the different 'nested loops',
 *                            e.g. 10 districts per warehouse results in {1, 10}
 * @param table               the column shall be added to this table as well as column metadata
 * @param name                the name of the column
 * @param generator_function  a lambda function to generate values for this column
 */
template <typename T>
void TableGenerator::add_column(std::shared_ptr<std::vector<size_t>> cardinalities,
                                std::shared_ptr<opossum::Table> table, std::string name,
                                const std::function<T(std::vector<size_t>)> &generator_function) {
  /**
   * We have to add Chunks when we add the first column.
   * This has to be made after the first column was created and added,
   * because empty Chunks would be pruned right away.
   */
  bool is_first_column = table->col_count() == 0;

  auto data_type_name = opossum::name_of_type<T>();
  table->add_column_definition(name, data_type_name);

  auto row_count = std::accumulate(std::begin(*cardinalities), std::end(*cardinalities), 1u, std::multiplies<size_t>());

  tbb::concurrent_vector<T> column;
  column.reserve(_chunk_size);

  for (size_t row_index = 0; row_index < row_count; row_index++) {
    std::vector<size_t> indices(cardinalities->size());

    // calculate indices for internal loops
    for (size_t loop = 0; loop < cardinalities->size(); loop++) {
      auto divisor = std::accumulate(std::begin(*cardinalities) + loop + 1, std::end(*cardinalities), 1u,
                                     std::multiplies<size_t>());
      indices[loop] = (row_index / divisor) % cardinalities->at(loop);
    }

    // actually generating and adding values
    column.push_back(generator_function(indices));

    // write output chunks if column size has reached chunk_size
    if (row_index % _chunk_size == _chunk_size - 1) {
      auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));
      opossum::ChunkID chunk_id{static_cast<uint32_t>(row_index / _chunk_size)};

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

  // write partially filled chunk
  if (row_count % _chunk_size != 0) {
    auto value_column = std::make_shared<opossum::ValueColumn<T>>(std::move(column));

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

std::shared_ptr<opossum::Table> TableGenerator::generate_items_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{NUM_ITEMS});

  /**
   * indices[0] = item
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(cardinalities, table, "I_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<int>(cardinalities, table, "I_IM_ID",
                  [&](std::vector<size_t>) -> size_t { return _random_gen.number(1, 10000); });
  add_column<std::string>(cardinalities, table, "I_NAME",
                          [&](std::vector<size_t>) { return _random_gen.astring(14, 24); });
  add_column<float>(cardinalities, table, "I_PRICE",
                    [&](std::vector<size_t>) { return _random_gen.number(100, 10000) / 100.f; });
  add_column<std::string>(cardinalities, table, "I_DATA", [&](std::vector<size_t> indices) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(indices[0]) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.number(0, data.length() - 1 - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  });

  opossum::DictionaryCompression::compress_table(*table);

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_warehouse_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size});

  /**
   * indices[0] = warehouse
   */

  add_column<int>(cardinalities, table, "W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<std::string>(cardinalities, table, "W_NAME",
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(cardinalities, table, "W_STREET_1",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "W_STREET_2",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "W_CITY",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "W_STATE",
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });
  add_column<std::string>(cardinalities, table, "W_ZIP", [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(cardinalities, table, "W_TAX",
                    [&](std::vector<size_t>) { return _random_gen.number(0, 2000) / 10000.f; });
  add_column<float>(cardinalities, table, "W_YTD",
                    [&](std::vector<size_t>) { return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT * NUM_DISTRICTS_PER_WAREHOUSE; });

  opossum::DictionaryCompression::compress_table(*table);

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_stock_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities =
      std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size, NUM_STOCK_ITEMS});

  /**
   * indices[0] = warehouse
   * indices[1] = stock
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(cardinalities, table, "S_I_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "S_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<int>(cardinalities, table, "S_QUANTITY",
                  [&](std::vector<size_t>) -> size_t { return _random_gen.number(10, 100); });
  for (int district_i = 1; district_i <= 10; district_i++) {
    std::stringstream district_i_str;
    district_i_str << std::setw(2) << std::setfill('0') << district_i;
    add_column<std::string>(cardinalities, table, "S_DIST_" + district_i_str.str(),
                            [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });
  }
  add_column<int>(cardinalities, table, "S_YTD", [&](std::vector<size_t>) { return 0; });
  add_column<int>(cardinalities, table, "S_ORDER_CNT", [&](std::vector<size_t>) { return 0; });
  add_column<int>(cardinalities, table, "S_REMOTE_CNT", [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(cardinalities, table, "S_DATA", [&](std::vector<size_t> indices) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(indices[1]) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.number(0, data.length() - 1 - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_district_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities =
      std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   */

  add_column<int>(cardinalities, table, "D_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "D_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<std::string>(cardinalities, table, "D_NAME",
                          [&](std::vector<size_t>) { return _random_gen.astring(6, 10); });
  add_column<std::string>(cardinalities, table, "D_STREET_1",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "D_STREET_2",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "D_CITY",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "D_STATE",
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });
  add_column<std::string>(cardinalities, table, "D_ZIP", [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<float>(cardinalities, table, "D_TAX",
                    [&](std::vector<size_t>) { return _random_gen.number(0, 2000) / 10000.f; });
  add_column<float>(cardinalities, table, "D_YTD", [&](std::vector<size_t>) { return CUSTOMER_YTD * NUM_CUSTOMERS_PER_DISTRICT; });
  add_column<int>(cardinalities, table, "D_NEXT_O_ID", [&](std::vector<size_t>) -> size_t { return NUM_ORDERS + 1; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_customer_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   */

  auto original_ids = _random_gen.select_unique_ids(NUM_ITEMS / 10, NUM_ITEMS);

  add_column<int>(cardinalities, table, "C_ID", [&](std::vector<size_t> indices) -> size_t { return indices[2]; });
  add_column<int>(cardinalities, table, "C_D_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "C_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<std::string>(cardinalities, table, "C_LAST", [&](std::vector<size_t> indices) -> std::string {
    return _random_gen.last_name(indices[2]);
  });
  add_column<std::string>(cardinalities, table, "C_MIDDLE", [&](std::vector<size_t>) { return "OE"; });
  add_column<std::string>(cardinalities, table, "C_FIRST",
                          [&](std::vector<size_t>) { return _random_gen.astring(8, 16); });
  add_column<std::string>(cardinalities, table, "C_STREET_1",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "C_STREET_2",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "C_CITY",
                          [&](std::vector<size_t>) { return _random_gen.astring(10, 20); });
  add_column<std::string>(cardinalities, table, "C_STATE",
                          [&](std::vector<size_t>) { return _random_gen.astring(2, 2); });
  add_column<std::string>(cardinalities, table, "C_ZIP", [&](std::vector<size_t>) { return _random_gen.zip_code(); });
  add_column<std::string>(cardinalities, table, "C_PHONE",
                          [&](std::vector<size_t>) { return _random_gen.nstring(16, 16); });
  add_column<int>(cardinalities, table, "C_SINCE", [&](std::vector<size_t>) { return _current_date; });
  add_column<std::string>(cardinalities, table, "C_CREDIT", [&](std::vector<size_t> indices) {
    bool is_original = original_ids.find(indices[2]) != original_ids.end();
    return is_original ? "BC" : "GC";
  });
  add_column<int>(cardinalities, table, "C_CREDIT_LIM", [&](std::vector<size_t>) { return 50000; });
  add_column<float>(cardinalities, table, "C_DISCOUNT",
                    [&](std::vector<size_t>) { return _random_gen.number(0, 5000) / 10000.f; });
  add_column<float>(cardinalities, table, "C_BALANCE", [&](std::vector<size_t>) { return -CUSTOMER_YTD; });
  add_column<float>(cardinalities, table, "C_YTD_PAYMENT", [&](std::vector<size_t>) { return CUSTOMER_YTD; });
  add_column<int>(cardinalities, table, "C_PAYMENT_CNT", [&](std::vector<size_t>) { return 1; });
  add_column<int>(cardinalities, table, "C_DELIVERY_CNT", [&](std::vector<size_t>) { return 0; });
  add_column<std::string>(cardinalities, table, "C_DATA",
                          [&](std::vector<size_t>) { return _random_gen.astring(300, 500); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_history_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_CUSTOMERS_PER_DISTRICT, NUM_HISTORY_ENTRIES});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = customer
   * indices[3] = history
   */

  add_column<int>(cardinalities, table, "H_C_ID", [&](std::vector<size_t> indices) -> size_t { return indices[2]; });
  add_column<int>(cardinalities, table, "H_C_D_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "H_C_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<int>(cardinalities, table, "H_DATE", [&](std::vector<size_t>) { return _current_date; });
  add_column<float>(cardinalities, table, "H_AMOUNT", [&](std::vector<size_t>) { return 10.f; });
  add_column<std::string>(cardinalities, table, "H_DATA",
                          [&](std::vector<size_t>) { return _random_gen.astring(12, 24); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

TableGenerator::order_line_counts_type TableGenerator::generate_order_line_counts() {
  order_line_counts_type v(_warehouse_size);
  for (auto &v_per_warehouse : v) {
    v_per_warehouse.resize(NUM_DISTRICTS_PER_WAREHOUSE);
    for (auto &v_per_district : v_per_warehouse) {
      v_per_district.resize(NUM_ORDERS);
      for (auto &v_per_order : v_per_district) {
        v_per_order = _random_gen.number(5, 15);
      }
    }
  }
  return v;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_order_table(
    TableGenerator::order_line_counts_type order_line_counts) {
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

  add_column<int>(cardinalities, table, "O_ID", [&](std::vector<size_t> indices) -> size_t { return indices[2]; });
  add_column<int>(cardinalities, table, "O_C_ID",
                  [&](std::vector<size_t> indices) -> size_t { return customer_permutation[indices[2]]; });
  add_column<int>(cardinalities, table, "O_D_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "O_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<int>(cardinalities, table, "O_ENTRY_D", [&](std::vector<size_t>) { return _current_date; });
  // TODO(anybody) -1 should be null
  add_column<int>(cardinalities, table, "O_CARRIER_ID", [&](std::vector<size_t> indices) {
    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _random_gen.number(1, 10) : -1;
  });
  add_column<int>(cardinalities, table, "O_OL_CNT", [&](std::vector<size_t> indices) -> size_t {
    return order_line_counts[indices[0]][indices[1]][indices[2]];
  });
  add_column<int>(cardinalities, table, "O_ALL_LOCAL", [&](std::vector<size_t>) -> size_t { return 1; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_order_line_table(
    TableGenerator::order_line_counts_type order_line_counts) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = order
   * indices[3] = order_line_size
   */

  add_column<int>(cardinalities, order_line_counts, table, "OL_O_ID",
                  [&](std::vector<size_t> indices) -> size_t { return indices[2]; });
  add_column<int>(cardinalities, order_line_counts, table, "OL_D_ID",
                  [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, order_line_counts, table, "OL_W_ID",
                  [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  add_column<int>(cardinalities, order_line_counts, table, "OL_NUMBER",
                  [&](std::vector<size_t> indices) -> size_t { return indices[3]; });
  add_column<int>(cardinalities, order_line_counts, table, "OL_I_ID",
                  [&](std::vector<size_t>) -> size_t { return _random_gen.number(1, NUM_ITEMS); });
  add_column<int>(cardinalities, order_line_counts, table, "OL_SUPPLY_W_ID",
                  [&](std::vector<size_t> indices) -> size_t { return indices[0]; });
  // TODO(anybody) -1 should be null
  add_column<int>(cardinalities, order_line_counts, table, "OL_DELIVERY_D", [&](std::vector<size_t> indices) {
    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? _current_date : -1;
  });
  add_column<int>(cardinalities, order_line_counts, table, "OL_QUANTITY",
                  [&](std::vector<size_t>) -> size_t { return 5; });
  add_column<float>(cardinalities, order_line_counts, table, "OL_AMOUNT", [&](std::vector<size_t> indices) -> float {
    return indices[2] <= NUM_ORDERS - NUM_NEW_ORDERS ? 0.f : _random_gen.number(1, 999999) / 100.f;
  });
  add_column<std::string>(cardinalities, order_line_counts, table, "OL_DIST_INFO",
                          [&](std::vector<size_t>) { return _random_gen.astring(24, 24); });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_new_order_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  auto cardinalities = std::make_shared<std::vector<size_t>>(
      std::initializer_list<size_t>{_warehouse_size, NUM_DISTRICTS_PER_WAREHOUSE, NUM_ORDERS});

  /**
   * indices[0] = warehouse
   * indices[1] = district
   * indices[2] = new_order
   */

  add_column<int>(cardinalities, table, "NO_O_ID", [&](std::vector<size_t> indices) -> size_t {
    return indices[2] + NUM_ORDERS + 1 - NUM_NEW_ORDERS;
  });
  add_column<int>(cardinalities, table, "NO_D_ID", [&](std::vector<size_t> indices) -> size_t { return indices[1]; });
  add_column<int>(cardinalities, table, "NO_W_ID", [&](std::vector<size_t> indices) -> size_t { return indices[0]; });

  opossum::DictionaryCompression::compress_table(*table);
  return table;
}

std::shared_ptr<std::map<std::string, std::shared_ptr<opossum::Table>>> TableGenerator::generate_all_tables() {
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

  return std::make_shared<std::map<std::string, std::shared_ptr<opossum::Table>>>(
      std::initializer_list<std::map<std::string, std::shared_ptr<opossum::Table>>::value_type>{
          {"ITEM", std::move(item_table)},
          {"WAREHOUSE", std::move(warehouse_table)},
          {"STOCK", std::move(stock_table)},
          {"DISTRICT", std::move(district_table)},
          {"CUSTOMER", std::move(customer_table)},
          {"HISTORY", std::move(history_table)},
          {"ORDER", std::move(order_table)},
          {"ORDER-LINE", std::move(order_line_table)},
          {"NEW-ORDER", std::move(new_order_table)}});
}

}  // namespace tpcc
