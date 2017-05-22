#include "table_generator.hpp"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "../lib/storage/value_column.hpp"

namespace tpcc {

TableGenerator::TableGenerator() : _random_gen(RandomGenerator()) {}

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

std::shared_ptr<opossum::Table> TableGenerator::generate_items_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("I_ID", "int", false);
  table->add_column("I_IM_ID", "int", false);
  table->add_column("I_NAME", "string", false);
  table->add_column("I_PRICE", "float", false);
  table->add_column("I_DATA", "string", false);

  auto original_ids = _random_gen.select_unique_ids(_item_size / 10, _item_size);

  auto chunk = opossum::Chunk();
  chunk.add_column(add_column<int>(_item_size, [](size_t i) { return i; }));
  chunk.add_column(add_column<int>(_item_size, [&](size_t) { return _random_gen.number(1, 10000); }));
  chunk.add_column(add_column<std::string>(_item_size, [&](size_t) { return _random_gen.astring(14, 24); }));
  chunk.add_column(add_column<float>(_item_size, [&](size_t) { return _random_gen.number(100, 10000) / 100.f; }));
  chunk.add_column(add_column<std::string>(_item_size, [&](size_t i) {
    std::string data = _random_gen.astring(26, 50);
    bool is_original = original_ids.find(i) != original_ids.end();
    if (is_original) {
      std::string originalString("ORIGINAL");
      size_t start_pos = _random_gen.number(0, data.length() - 1 - originalString.length());
      data.replace(start_pos, originalString.length(), originalString);
    }
    return data;
  }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_warehouse_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

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

  auto chunk = opossum::Chunk();
  chunk.add_column(add_column<int>(_warehouse_size, [](size_t i) { return i; }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(6, 10); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(10, 20); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.astring(2, 2); }));
  chunk.add_column(add_column<std::string>(_warehouse_size, [&](size_t) { return _random_gen.zipCode(); }));
  chunk.add_column(add_column<float>(_warehouse_size, [&](size_t) { return _random_gen.number(0, 2000) / 10000.f; }));
  chunk.add_column(
      add_column<float>(_warehouse_size, [&](size_t) { return _customer_ytd * _customer_size * _district_size; }));

  table->add_chunk(std::move(chunk));

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_stock_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("S_I_ID", "int", false);
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

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    auto original_ids = _random_gen.select_unique_ids(_item_size / 10, _item_size);

    auto chunk = opossum::Chunk();
    chunk.add_column(add_column<int>(_stock_size, [](size_t i) { return i; }));
    chunk.add_column(add_column<int>(_stock_size, [&](size_t) { return warehouse_id; }));
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
        size_t start_pos = _random_gen.number(0, data.length() - 1 - originalString.length());
        data.replace(start_pos, originalString.length(), originalString);
      }
      return data;
    }));

    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_district_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

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

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    auto chunk = opossum::Chunk();
    chunk.add_column(add_column<int>(_district_size, [](size_t i) { return i; }));
    chunk.add_column(add_column<int>(_district_size, [&](size_t) { return warehouse_id; }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(6, 10); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(10, 20); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.astring(2, 2); }));
    chunk.add_column(add_column<std::string>(_district_size, [&](size_t) { return _random_gen.zipCode(); }));
    chunk.add_column(add_column<float>(_district_size, [&](size_t) { return _random_gen.number(0, 2000) / 10000.f; }));
    chunk.add_column(add_column<float>(_district_size, [&](size_t) { return _customer_ytd * _customer_size; }));
    chunk.add_column(add_column<int>(_district_size, [&](size_t) { return _order_size + 1; }));

    table->add_chunk(std::move(chunk));
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_customer_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

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

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    for (size_t district_id = 0; district_id < _district_size; district_id++) {
      auto original_ids = _random_gen.select_unique_ids(_item_size / 10, _item_size);

      auto chunk = opossum::Chunk();
      chunk.add_column(add_column<int>(_customer_size, [](size_t i) { return i; }));
      chunk.add_column(add_column<int>(_customer_size, [&](size_t) { return district_id; }));
      chunk.add_column(add_column<int>(_customer_size, [&](size_t) { return warehouse_id; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t i) { return _random_gen.last_name(i); }));
      chunk.add_column(add_column<std::string>(_customer_size, [](size_t) { return "OE"; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(8, 16); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(10, 20); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(2, 2); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.zipCode(); }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.nstring(16, 16); }));
      chunk.add_column(add_column<int>(_customer_size, [&](size_t) { return _current_date; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t i) {
        bool is_original = original_ids.find(i) != original_ids.end();
        return is_original ? "BC" : "GC";
      }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 50000; }));
      chunk.add_column(
          add_column<float>(_customer_size, [&](size_t) { return _random_gen.number(0, 5000) / 10000.f; }));
      chunk.add_column(add_column<float>(_customer_size, [&](size_t) { return -_customer_ytd; }));
      chunk.add_column(add_column<float>(_customer_size, [&](size_t) { return _customer_ytd; }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 1; }));
      chunk.add_column(add_column<int>(_customer_size, [](size_t) { return 0; }));
      chunk.add_column(add_column<std::string>(_customer_size, [&](size_t) { return _random_gen.astring(300, 500); }));

      table->add_chunk(std::move(chunk));
    }
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_history_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("H_C_ID", "int", false);
  table->add_column("H_C_D_ID", "int", false);
  table->add_column("H_C_W_ID", "int", false);
  table->add_column("H_DATE", "int", false);
  table->add_column("H_AMOUNT", "float", false);
  table->add_column("H_DATA", "string", false);

  auto _history_size_per_district = _history_size * _customer_size;

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    for (size_t district_id = 0; district_id < _district_size; district_id++) {
      auto chunk = opossum::Chunk();
      chunk.add_column(add_column<int>(_history_size_per_district, [&](size_t i) { return i / _history_size; }));
      chunk.add_column(add_column<int>(_history_size_per_district, [&](size_t) { return district_id; }));
      chunk.add_column(add_column<int>(_history_size_per_district, [&](size_t) { return warehouse_id; }));
      chunk.add_column(add_column<int>(_history_size_per_district, [&](size_t) { return _current_date; }));
      chunk.add_column(add_column<float>(_history_size_per_district, [](size_t) { return 10.f; }));
      chunk.add_column(
          add_column<std::string>(_history_size_per_district, [&](size_t) { return _random_gen.astring(12, 24); }));
      table->add_chunk(std::move(chunk));
    }
  }

  return table;
}

TableGenerator::order_line_counts_type TableGenerator::generate_order_line_counts() {
  order_line_counts_type v(_warehouse_size);
  for (auto &v_per_warehouse : v) {
    v_per_warehouse.resize(_district_size);
    for (auto &v_per_district : v_per_warehouse) {
      v_per_district.resize(_order_size);
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

  // setup columns
  table->add_column("O_ID", "int", false);
  table->add_column("O_C_ID", "int", false);
  table->add_column("O_D_ID", "int", false);
  table->add_column("O_W_ID", "int", false);
  table->add_column("O_ENTRY_D", "int", false);
  table->add_column("O_CARRIER_ID", "int", false);
  table->add_column("O_OL_CNT", "int", false);
  table->add_column("O_ALL_LOCAL", "int", false);

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    for (size_t district_id = 0; district_id < _district_size; district_id++) {
      auto customer_permutation = _random_gen.permutation(0, _customer_size);
      auto chunk = opossum::Chunk();
      chunk.add_column(add_column<int>(_order_size, [](size_t i) { return i; }));
      chunk.add_column(add_column<int>(_order_size, [&](size_t i) { return customer_permutation[i]; }));
      chunk.add_column(add_column<int>(_order_size, [&](size_t) { return district_id; }));
      chunk.add_column(add_column<int>(_order_size, [&](size_t) { return warehouse_id; }));
      chunk.add_column(add_column<int>(_order_size, [&](size_t) { return _current_date; }));
      // TODO(anybody) -1 should be null
      chunk.add_column(add_column<int>(
          _order_size, [&](size_t i) { return i <= _order_size - _new_order_size ? _random_gen.number(1, 10) : -1; }));
      chunk.add_column(
          add_column<int>(_order_size, [&](size_t i) { return order_line_counts[warehouse_id][district_id][i]; }));
      chunk.add_column(add_column<int>(_order_size, [](size_t) { return 1; }));
      table->add_chunk(std::move(chunk));
    }
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_order_line_table(
    TableGenerator::order_line_counts_type order_line_counts) {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("OL_O_ID", "int", false);
  table->add_column("OL_D_ID", "int", false);
  table->add_column("OL_W_ID", "int", false);
  table->add_column("OL_NUMBER", "int", false);
  table->add_column("OL_I_ID", "int", false);
  table->add_column("OL_SUPPLY_W_ID", "int", false);
  table->add_column("OL_DELIVERY_D", "int", false);
  table->add_column("OL_QUANTITY", "int", false);
  table->add_column("OL_AMOUNT", "float", false);
  table->add_column("OL_DIST_INFO", "string", false);

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    for (size_t district_id = 0; district_id < _district_size; district_id++) {
      for (size_t order_id = 0; order_id < _order_size; order_id++) {
        auto chunk = opossum::Chunk();
        auto order_line_size = order_line_counts[warehouse_id][district_id][order_id];
        chunk.add_column(add_column<int>(order_line_size, [&](size_t) { return order_id; }));
        chunk.add_column(add_column<int>(order_line_size, [&](size_t) { return district_id; }));
        chunk.add_column(add_column<int>(order_line_size, [&](size_t) { return warehouse_id; }));
        chunk.add_column(add_column<int>(order_line_size, [](size_t i) { return i; }));
        chunk.add_column(add_column<int>(order_line_size, [&](size_t) { return _random_gen.number(1, _item_size); }));
        chunk.add_column(add_column<int>(order_line_size, [&](size_t) { return warehouse_id; }));
        // TODO(anybody) -1 should be null
        chunk.add_column(add_column<int>(
            order_line_size, [&](size_t) { return order_id <= _order_size - _new_order_size ? _current_date : -1; }));
        chunk.add_column(add_column<int>(order_line_size, [](size_t) { return 5; }));
        chunk.add_column(add_column<float>(order_line_size, [&](size_t) {
          return order_id <= _order_size - _new_order_size ? 0.f : _random_gen.number(1, 999999) / 100.f;
        }));
        chunk.add_column(add_column<std::string>(order_line_size, [&](size_t) { return _random_gen.astring(24, 24); }));
        table->add_chunk(std::move(chunk));
      }
    }
  }

  return table;
}

std::shared_ptr<opossum::Table> TableGenerator::generate_new_order_table() {
  auto table = std::make_shared<opossum::Table>(_chunk_size);

  // setup columns
  table->add_column("NO_O_ID", "int", false);
  table->add_column("NO_D_ID", "int", false);
  table->add_column("NO_W_ID", "int", false);

  for (size_t warehouse_id = 0; warehouse_id < _warehouse_size; warehouse_id++) {
    for (size_t district_id = 0; district_id < _district_size; district_id++) {
      auto chunk = opossum::Chunk();
      chunk.add_column(
          add_column<int>(_new_order_size, [&](size_t i) { return i + _order_size + 1 - _new_order_size; }));
      chunk.add_column(add_column<int>(_new_order_size, [&](size_t) { return district_id; }));
      chunk.add_column(add_column<int>(_new_order_size, [&](size_t) { return warehouse_id; }));
      table->add_chunk(std::move(chunk));
    }
  }

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
