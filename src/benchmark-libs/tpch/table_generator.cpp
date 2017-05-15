#include "table_generator.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "../lib/storage/value_column.hpp"

namespace tpch {

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
  chunk.add_column(add_column<std::string>(table_size, [](size_t) { return ""; }));  // TODO(anybody)
  // S_ADDRESS
  chunk.add_column(add_column<std::string>(table_size, [](size_t) { return ""; }));  // TODO(anybody)
  // S_NATIONKEY
  chunk.add_column(add_column<int>(table_size, [&](size_t) { return _random_gen.number(0, 24); }));
  // S_PHONE
  chunk.add_column(add_column<std::string>(table_size, [](size_t) { return ""; }));  // TODO(anybody)
  // S_ACCTBAL
  chunk.add_column(add_column<float>(table_size, [&](size_t) { return _random_gen.number(-99999, 999999) / 100.f; }));
  // S_COMMENT
  chunk.add_column(add_column<std::string>(table_size, [](size_t) { return ""; }));  // TODO(anybody)

  table->add_chunk(std::move(chunk));

  return table;
}

void TableGenerator::add_all_tables(opossum::StorageManager &manager) {
  auto supplier_table = generate_suppliers_table();

  manager.add_table("SUPPLIER", std::move(supplier_table));
}

}  // namespace tpch
