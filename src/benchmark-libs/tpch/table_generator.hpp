#pragma once

#include <time.h>
#include <memory>
#include <string>
#include <vector>
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "random_generator.hpp"
#include "text_field_generator.hpp"

namespace tpch {

class TableGenerator {
  // following TPC-H v2.17.2
 public:
  TableGenerator();

  virtual ~TableGenerator() = default;

  std::shared_ptr<opossum::Table> generate_suppliers_table();

  std::shared_ptr<opossum::Table> generate_parts_table();

  void add_all_tables(opossum::StorageManager &manager);

  const size_t _chunk_size = 1000;

  const size_t _scale_factor = 1;

  const size_t _supplier_size = 10000;  // * _scale_factor
  const size_t _part_size = 200000;     // * _scale_factor

 protected:
  template <typename T>
  std::shared_ptr<opossum::ValueColumn<T>> add_column(size_t cardinality,
                                                      const std::function<T(size_t)> &generator_function);

  RandomGenerator _random_gen;
  TextFieldGenerator _text_field_gen;
};
}  // namespace tpch
