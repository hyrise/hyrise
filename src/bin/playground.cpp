#include <iostream>

#include "operators/get_table.hpp"
#include "operators/join_index.hpp"

#include "storage/chunk.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto left_table = std::make_shared<Table>(TableColumnDefinitions{{"column", DataType::Int}}, TableType::Data,
                                                  100'000, UseMvcc::Yes);

  const auto right_table = std::make_shared<Table>(TableColumnDefinitions{{"column", DataType::Int}}, TableType::Data,
                                                   100'000, UseMvcc::Yes);

  // Create table with 600'000 rows
  for (int i = 0; i < 600'000; i++) {
    left_table->append({i});
  }

  for (int i = 0; i < 60'000; i++) {
    right_table->append({i});
  }

  // Create indices
  const auto left_chunks = left_table->chunks();
  for (const auto& chunk : left_chunks) {
    chunk->template create_index<BTreeIndex>(std::vector<ColumnID>{ColumnID{0}});
  }
  const auto right_chunks = right_table->chunks();
  for (const auto& chunk : right_chunks) {
    chunk->template create_index<BTreeIndex>(std::vector<ColumnID>{ColumnID{0}});
  }
  StorageManager::get().add_table("left_table", left_table);
  StorageManager::get().add_table("right_table", right_table);

  // Run IndexJoin
  auto left_gt = std::make_shared<GetTable>("left_table");
  left_gt->execute();
  auto right_gt = std::make_shared<GetTable>("right_table");
  right_gt->execute();
  auto join =
      std::make_shared<JoinIndex>(left_gt, right_gt, JoinMode::Inner,
                                  std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  // Will crash with GCC, but not with Clang
  join->execute();

  return 0;
}
