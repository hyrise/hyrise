
#include <array>
#include <memory>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tasks/chunk_compression_task.hpp"

namespace opossum {

class ChunkCompressionTaskTest : public BaseTest {};

TEST_F(ChunkCompressionTaskTest, CompressionPreservesTableContent) {
  auto table = load_table("src/test/tables/compression_input.tbl", 12u);
  StorageManager::get().add_table("table", table);

  auto table_dict = load_table("src/test/tables/compression_input.tbl", 3u);
  StorageManager::get().add_table("table_dict", table_dict);

  auto compression_task1 = std::make_unique<ChunkCompressionTask>("table_dict", ChunkID{0});
  compression_task1->set_done_callback([]() {
    auto compression_task2 =
        std::make_unique<ChunkCompressionTask>("table_dict", std::vector<ChunkID>{ChunkID{1}, ChunkID{2}});
    compression_task2->execute();
  });
  compression_task1->execute();
  auto compression_task3 = std::make_unique<ChunkCompressionTask>("table_dict", ChunkID{3});
  compression_task3->execute();

  ASSERT_TRUE(check_table_equal(table, table_dict, OrderSensitivity::No, TypeCmpMode::Strict,
                                FloatComparisonMode::AbsoluteDifference));

  constexpr auto chunk_count = 4u;
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table_dict->get_chunk(chunk_id);

    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      auto column = chunk->get_column(column_id);

      auto dict_column = std::dynamic_pointer_cast<const BaseDictionaryColumn>(column);
      ASSERT_NE(dict_column, nullptr);
    }
  }
}

TEST_F(ChunkCompressionTaskTest, DictionarySize) {
  auto table_dict = load_table("src/test/tables/compression_input.tbl", 6u);
  StorageManager::get().add_table("table_dict", table_dict);

  auto compression = std::make_unique<ChunkCompressionTask>("table_dict", std::vector<ChunkID>{ChunkID{0}, ChunkID{1}});
  compression->execute();

  constexpr auto chunk_count = 2u;

  ASSERT_EQ(table_dict->chunk_count(), chunk_count);

  auto dictionary_sizes = std::array<std::vector<size_t>, chunk_count>{{{3u, 3u}, {2u, 3u}}};

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table_dict->get_chunk(chunk_id);
    for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
      auto column = chunk->get_column(column_id);

      auto dict_column = std::dynamic_pointer_cast<const BaseDictionaryColumn>(column);
      ASSERT_NE(dict_column, nullptr);

      EXPECT_EQ(dict_column->unique_values_count(), dictionary_sizes[chunk_id][column_id]);
    }
  }
}

TEST_F(ChunkCompressionTaskTest, CompressionWithAbortedInsert) {
  auto table = load_table("src/test/tables/compression_input.tbl", 6u);
  StorageManager::get().add_table("table_insert", table);

  auto gt1 = std::make_shared<GetTable>("table_insert");
  gt1->execute();

  auto ins = std::make_shared<Insert>("table_insert", gt1);
  auto context = TransactionManager::get().new_transaction_context();
  ins->set_transaction_context(context);
  ins->execute();
  context->rollback();

  ASSERT_EQ(table->chunk_count(), 4u);

  auto compression = std::make_unique<ChunkCompressionTask>(
      "table_insert", std::vector<ChunkID>{ChunkID{0}, ChunkID{1}, ChunkID{2}, ChunkID{3}});
  compression->execute();

  for (auto i = ChunkID{0}; i < table->chunk_count() - 1; ++i) {
    auto dict_column =
        std::dynamic_pointer_cast<const BaseDictionaryColumn>(table->get_chunk(i)->get_column(ColumnID{0}));
    ASSERT_NE(dict_column, nullptr);
  }

  auto gt2 = std::make_shared<GetTable>("table_insert");
  gt2->execute();
  auto validate = std::make_shared<Validate>(gt2);
  context = TransactionManager::get().new_transaction_context();
  validate->set_transaction_context(context);
  validate->execute();
  EXPECT_EQ(validate->get_output()->row_count(), 12u);
}

}  // namespace opossum
