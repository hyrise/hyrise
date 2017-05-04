#include "tpcc_table_generator.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "storage/value_column.hpp"

namespace opossum {

TPCCTableGenerator::TPCCTableGenerator() : _random_gen(RandomGenerator()) {}

template <typename T>
std::shared_ptr<ValueColumn<T>> TPCCTableGenerator::add_column(size_t cardinality,
                                                               const std::function<T(size_t)> &generator_function) {
  tbb::concurrent_vector<T> column(cardinality);
  for (size_t i = 0; i < column.size(); i++) {
    column[i] = generator_function(i);
  }
  return std::make_shared<ValueColumn<T>>(std::move(column));
}

std::shared_ptr<Table> TPCCTableGenerator::generate_items_table() {
  auto table = std::make_shared<Table>(_chunk_size);

  // setup columns
  table->add_column("I_ID", "int", false);
  table->add_column("I_IM_ID", "int", false);
  table->add_column("I_NAME", "string", false);
  table->add_column("I_PRICE", "float", false);
  table->add_column("I_DATA", "string", false);

  auto original_ids = _random_gen.select_unique_ids(_item_size / 10, 1, _item_size);

  auto chunk = Chunk();
  chunk.add_column(add_column<int>(_item_size, [](size_t i) { return i; }));
  chunk.add_column(add_column<int>(_item_size, [&](size_t) { return _random_gen.number(1, 10000); }));
  chunk.add_column(add_column<std::string>(_item_size, [&](size_t) { return _random_gen.astring(14, 24); }));
  chunk.add_column(add_column<float>(_item_size, [&](size_t) { return _random_gen.number(100, 10000) / 100.0; }));
  chunk.add_column(add_column<std::string>(_item_size, [&](size_t i) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(i) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.number(0, data.length() - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_warehouse_table() {
  auto table = std::make_shared<Table>(_chunk_size);

  // setup columns
  table->add_column("W_ID", "int", false);
  table->add_column("W_NAME", "string", false);
  table->add_column("W_STREET_1", "string", false);
  table->add_column("W_STREET_2", "string", false);
  table->add_column("W_CITY", "string", false);
  table->add_column("W_STATE", "string", false);
  table->add_column("W_ZIP", "int", false);
  table->add_column("W_TAX", "float", false);
  table->add_column("W_YTD", "float", false);

  auto chunk = Chunk();
  chunk.add_column(add_column<int>(_warehouse_size, [](size_t i) { return i; }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(6, 10); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(2, 2); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.zipCode(); }));
  chunk.add_column(add_column<float>(_warehouse_size, [&](size_t) { return _random_gen.number(0, 2000) / 10000.0; }));
  chunk.add_column(add_column<float>(_warehouse_size, [&](size_t) { return 30000.0; }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_stock_table() {
  auto table = std::make_shared<Table>(_chunk_size);

  // setup columns
  table->add_column("S_ID", "int", false);
  table->add_column("S_W_ID", "int", false);
  table->add_column("S_QUANTITY", "int", false);
  table->add_column("S_DIST_01", "string", false);
  table->add_column("S_DIST_02", "string", false);
  table->add_column("S_DIST_03", "string", false);
  table->add_column("S_DIST_04", "string", false);
  table->add_column("S_DIST_05", "string", false);
  table->add_column("S_DIST_06", "string", false);
  table->add_column("S_DIST_07", "string", false);
  table->add_column("S_DIST_08", "string", false);
  table->add_column("S_DIST_09", "string", false);
  table->add_column("S_DIST_10", "string", false);
  table->add_column("S_YTD", "int", false);
  table->add_column("S_ORDER_CNT", "int", false);
  table->add_column("S_REMOTE_CNT", "int", false);
  table->add_column("S_DATA", "string", false);

  for (size_t t = 0; t < _warehouse_size; t++) {
    auto original_ids = _random_gen.select_unique_ids(_item_size / 10, 1, _item_size);

    auto chunk = Chunk();
    chunk.add_column(add_column<int>(_stock_size, [](size_t i) { return i; }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return t; }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return _random_gen.number(10, 100); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t) { return _random_gen.astring(24, 24); }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return 0; }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return 0; }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return 0; }));
    chunk.add_column(add_column<std::string>(_stock_size, [&](size_t i) {
      std::string data = _random_gen.astring(26, 50);
      bool is_original = original_ids.find(i) != original_ids.end();
      if (is_original) {
        std::string originalString("ORIGINAL");
        size_t start_pos = _random_gen.number(0, data.length() - originalString.length());
        data.replace(start_pos, originalString.length(), originalString);
      }
      return data;
    }));

    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_district_table() {
  auto table = std::make_shared<Table>(_chunk_size);

  // setup columns
  table->add_column("D_ID", "int", false);
  table->add_column("D_W_ID", "int", false);
  table->add_column("D_NAME", "string", false);
  table->add_column("D_STREET_1", "string", false);
  table->add_column("D_STREET_2", "string", false);
  table->add_column("D_CITY", "string", false);
  table->add_column("D_STATE", "string", false);
  table->add_column("D_ZIP", "string", false);
  table->add_column("D_TAX", "float", false);
  table->add_column("D_YTD", "float", false);
  table->add_column("D_NEXT_O_ID", "int", false);

  for (size_t t = 0; t < _warehouse_size; t++) {
    auto chunk = Chunk();
    chunk.add_column(add_column<int>(_district_size, [](size_t i) { return i; }));
    chunk.add_column(add_column<int>(_district_size, [&](size_t) { return t; }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(6, 10); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(2, 2); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.zipCode(); }));
    chunk.add_column(add_column<float>(_district_size, [&](size_t) { return _random_gen.number(0, 2000) / 10000.0; }));
    chunk.add_column(add_column<float>(_district_size, [&](size_t) { return 30000.0; }));
    chunk.add_column(add_column<int>(_district_size, [&](size_t) { return 3001; }));

    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<Table> TPCCTableGenerator::generate_customer_table() {
  auto table = std::make_shared<Table>(_chunk_size);

  // setup columns
  table->add_column("C_ID", "int", false);
  table->add_column("C_D_ID", "int", false);
  table->add_column("C_W_ID", "int", false);
  table->add_column("C_LAST", "string", false);
  table->add_column("C_MIDDLE", "string", false);
  table->add_column("C_FIRST", "string", false);
  table->add_column("C_STREET_1", "string", false);
  table->add_column("C_STREET_2", "string", false);
  table->add_column("C_CITY", "string", false);
  table->add_column("C_STATE", "string", false);
  table->add_column("C_ZIP", "string", false);
  table->add_column("C_PHONE", "string", false);
  table->add_column("C_SINCE", "int", false);
  table->add_column("C_CREDIT", "string", false);
  table->add_column("C_CREDIT_LIM", "int", false);
  table->add_column("C_DISCOUNT", "float", false);
  table->add_column("C_BALANCE", "float", false);
  table->add_column("C_YTD_PAYMENT", "float", false);
  table->add_column("C_PAYMENT_CNT", "int", false);
  table->add_column("C_DELIVERY_CNT", "int", false);
  table->add_column("C_DATA", "string", false);

  for (size_t s = 0; s < _warehouse_size; s++) {
    for (size_t t = 0; t < _district_size; t++) {
      auto original_ids = _random_gen.select_unique_ids(_item_size / 10, 1, _item_size);

      auto chunk = Chunk();
      chunk.add_column(add_column<int>(_customer_size, [](size_t i) { return i; }));
      chunk.add_column(add_column<int>(_customer_size, [&](size_t) { return t; }));
      chunk.add_column(add_column<int>(_customer_size, [&](size_t) { return s; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.last_name(); }));
      chunk.add_column(add_column<std::string>(_customer_size, [](size_t) { return "OE"; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(8, 16); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(2, 2); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.zipCode(); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.nstring(16, 16); }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 0; }));  // TODO(anyone): Add time now
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t i) {
        bool is_original = original_ids.find(i) != original_ids.end();
        return is_original ? "BC" : "GC";
      }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 50000; }));
      chunk.add_column(
          add_column<float>(_customer_size, [&](size_t) { return _random_gen.number(0, 5000) / 10000.0; }));
      chunk.add_column(add_column<float>(_customer_size, [](size_t) { return -10.00; }));
      chunk.add_column(add_column<float>(_customer_size, [](size_t) { return 10.00; }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 1; }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 0; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(300, 500); }));

      table->add_chunk(std::move(chunk));
    }
  }

  return table;
}

}  // namespace opossum
