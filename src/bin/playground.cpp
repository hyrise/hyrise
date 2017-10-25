#include <iostream>

#include "operators/print.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

int main() {
  if (!opossum::StorageManager::get().has_table("test")) {
    opossum::pmr_concurrent_vector<int32_t> values({1, 2, 3});

    opossum::Chunk chunk(true);
    chunk.add_column(std::make_shared<opossum::ValueColumn<int32_t>>(std::move(values)));

    auto test_table = std::make_shared<opossum::Table>();
    test_table->add_column_definition("a", "int");
    test_table->emplace_chunk(std::move(chunk));

    opossum::StorageManager::get().add_table("test", test_table);
    opossum::Print::print(test_table);
  }

  return 0;
}
