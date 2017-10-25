#include <iostream>
#include <storage/reference_column.hpp>

#include "operators/print.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/reference_column.hpp"
#include "storage/storage_manager.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

int main() {
  if (!opossum::StorageManager::get().has_table("test")) {
    auto pos_list = std::make_shared<opossum::PosList>({opossum::RowID{opossum::ChunkID(0), opossum::ChunkOffset(0)}});

    opossum::Chunk chunk(true);
    chunk.add_column(std::make_shared<opossum::ReferenceColumn>(std::move(values)));

    auto test_table = std::make_shared<opossum::Table>();
    test_table->add_column_definition("a", "int");
    test_table->emplace_chunk(std::move(chunk));

    opossum::StorageManager::get().add_table("test", test_table);
    opossum::Print::print(test_table);
  }

  return 0;
}
