#include "table_generator.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "../lib/storage/value_column.hpp"

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
  chunk.add_column(add_column<std::string>(table_size, [&](size_t i) { return "Brand#" + manufacturers[i] + std::to_string(_random_gen.number(1, 5)); }));
  // P_TYPE
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.part_type(); }));
  // P_SIZE
  chunk.add_column(add_column<int>(table_size, [&](size_t) { return _random_gen.number(1, 50); }));
  // P_CONTAINER
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.part_container(); }));
  // P_RETAILPRICE
  chunk.add_column(add_column<float>(table_size, [](size_t i) { return (90000.f + (i % 200001 )/10.f + 100.f * (i % 1000))/100.f; }));
  // P_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [&](size_t) { return _text_field_gen.text_string(5, 22); }));

  table->add_chunk(std::move(chunk));

  return table;
}

void TableGenerator::add_all_tables(opossum::StorageManager &manager) {
  auto supplier_table = generate_suppliers_table();
  auto parts_table = generate_parts_table();

  manager.add_table("SUPPLIER", std::move(supplier_table));
  manager.add_table("PARTS", std::move(parts_table));
}

}  // namespace tpch
