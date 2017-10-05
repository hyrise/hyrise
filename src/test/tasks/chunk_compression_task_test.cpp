
#include <array>
#include <memory>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/base_dictionary_column.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/tasks/chunk_compression_task.hpp"

namespace opossum {

class ChunkCompressionTaskTest : public BaseTest {
 protected:
  void SetUp() override {}

 private:
};

TEST_F(ChunkCompressionTaskTest, CompressionPreservesTableContent) {
  auto table = load_table("src/test/tables/compression_input.tbl", 6u);
  StorageManager::get().add_table("table", table);

  auto table_dict = load_table("src/test/tables/compression_input.tbl", 6u);
  StorageManager::get().add_table("table_dict", table_dict);

  auto compression =
      std::make_unique<ChunkCompressionTask>("table_dict", std::vector<ChunkID>{ChunkID{0}, ChunkID{1}}, false);
  compression->execute();

  ASSERT_TABLE_EQ(table, table_dict);
}

TEST_F(ChunkCompressionTaskTest, DictionarySize) {
  auto table_dict = load_table("src/test/tables/compression_input.tbl", 6u);
  StorageManager::get().add_table("table_dict", table_dict);

  auto compression =
      std::make_unique<ChunkCompressionTask>("table_dict", std::vector<ChunkID>{ChunkID{0}, ChunkID{1}}, false);
  compression->execute();

  constexpr auto chunk_count = 2u;

  ASSERT_EQ(table_dict->chunk_count(), chunk_count);

  auto dictionary_sizes = std::array<std::vector<size_t>, chunk_count>{{{3u, 3u}, {2u, 3u}}};

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto& chunk = table_dict->get_chunk(chunk_id);

    for (ColumnID column_id{0}; column_id < chunk.col_count(); ++column_id) {
      auto column = chunk.get_column(column_id);

      auto dict_column = std::dynamic_pointer_cast<BaseDictionaryColumn>(column);
      ASSERT_NE(dict_column, nullptr);

      EXPECT_EQ(dict_column->unique_values_count(), dictionary_sizes[chunk_id][column_id]);
    }
  }
}

}  // namespace opossum
