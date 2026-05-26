
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/validate.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "tasks/chunk_compression_task.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class ChunkCompressionTaskTest : public BaseTest {};

TEST_F(ChunkCompressionTaskTest, CompressionPreservesTableContent) {
  const auto table = load_table("resources/test_data/tbl/compression_input.tbl", ChunkOffset{12});
  const auto table_dict = load_table("resources/test_data/tbl/compression_input.tbl", ChunkOffset{3});

  const auto compression_task1 = std::make_shared<ChunkCompressionTask>(table_dict, ChunkID{0});
  compression_task1->set_done_callback([table_dict]() {
    const auto compression_task2 =
        std::make_shared<ChunkCompressionTask>(table_dict, std::vector<ChunkID>{ChunkID{1}, ChunkID{2}});
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task2});
  });
  const auto compression_task3 = std::make_shared<ChunkCompressionTask>(table_dict, ChunkID{3});

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task1, compression_task3});

  EXPECT_TABLE_EQ_UNORDERED(table, table_dict);

  constexpr auto chunk_count = size_t{4};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table_dict->get_chunk(chunk_id);
    EXPECT_TRUE(std::dynamic_pointer_cast<const BaseDictionarySegment>(chunk->get_segment(ColumnID{0})));
    EXPECT_TRUE(std::dynamic_pointer_cast<const FrameOfReferenceSegment<int32_t>>(chunk->get_segment(ColumnID{1})));
  }
}

TEST_F(ChunkCompressionTaskTest, DictionarySize) {
  const auto table_dict = load_table("resources/test_data/tbl/compression_input.tbl", ChunkOffset{6});

  const auto compression = std::make_shared<ChunkCompressionTask>(
      table_dict, std::vector<ChunkID>{ChunkID{0}, ChunkID{1}},
      ChunkEncodingSpec{SegmentEncodingSpec{EncodingType::Dictionary}, SegmentEncodingSpec{EncodingType::Dictionary}});
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression});

  constexpr auto chunk_count = 2;

  ASSERT_EQ(table_dict->chunk_count(), chunk_count);

  const auto dictionary_sizes = std::array<std::vector<size_t>, chunk_count>{{{3, 3}, {2, 3}}};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table_dict->get_chunk(chunk_id);
    for (auto column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      const auto dict_segment = std::dynamic_pointer_cast<const BaseDictionarySegment>(chunk->get_segment(column_id));
      ASSERT_TRUE(dict_segment);
      EXPECT_EQ(dict_segment->unique_values_count(), dictionary_sizes[chunk_id][column_id]);
    }
  }
}

TEST_F(ChunkCompressionTaskTest, CompressionWithAbortedInsert) {
  const auto table = load_table("resources/test_data/tbl/compression_input.tbl", ChunkOffset{6});
  Hyrise::get().storage_manager.add_table("table_insert", table);

  const auto get_table_1 = std::make_shared<GetTable>("table_insert");
  get_table_1->execute();

  const auto insert = std::make_shared<Insert>("table_insert", get_table_1);
  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  insert->set_transaction_context(context);
  insert->execute();
  context->rollback(RollbackReason::User);

  ASSERT_EQ(table->chunk_count(), 4);

  // The last two chunks were created by the Insert operator. Even though it was rolled back, it marks the first chunk
  // it created as immutable. The last created chunk was not marked yet.
  EXPECT_FALSE(table->get_chunk(ChunkID{2})->is_mutable());
  EXPECT_FALSE(table->get_chunk(ChunkID{3})->is_mutable());

  const auto compression = std::make_shared<ChunkCompressionTask>(
      table, std::vector<ChunkID>{ChunkID{0}, ChunkID{1}, ChunkID{2}, ChunkID{3}});
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression});

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count() - 1; ++chunk_id) {
    const auto segment = table->get_chunk(chunk_id)->get_segment(ColumnID{0});
    EXPECT_TRUE(std::dynamic_pointer_cast<const BaseDictionarySegment>(segment));
  }

  const auto get_table_2 = std::make_shared<GetTable>("table_insert");
  get_table_2->execute();
  const auto validate = std::make_shared<Validate>(get_table_2);
  const auto context_2 = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  validate->set_transaction_context(context_2);
  validate->execute();
  EXPECT_EQ(validate->get_output()->row_count(), 12);
}

TEST_F(ChunkCompressionTaskTest, IgnoreDeletedChunk) {
  const auto table = load_table("resources/test_data/tbl/compression_input.tbl", ChunkOffset{8});
  table->get_chunk(ChunkID{0})->increase_invalid_row_count(ChunkOffset{8});
  table->remove_chunk(ChunkID{0});

  const auto compression_task =
      std::make_shared<ChunkCompressionTask>(table, std::vector<ChunkID>{ChunkID{0}, ChunkID{1}});
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task});

  ASSERT_EQ(table->chunk_count(), 2);
  EXPECT_FALSE(table->get_chunk(ChunkID{0}));
  const auto encoded_chunk = table->get_chunk(ChunkID{1});
  ASSERT_TRUE(encoded_chunk);
  ASSERT_EQ(encoded_chunk->column_count(), 2);
  EXPECT_TRUE(std::dynamic_pointer_cast<AbstractEncodedSegment>(encoded_chunk->get_segment(ColumnID{0})));
  EXPECT_TRUE(std::dynamic_pointer_cast<AbstractEncodedSegment>(encoded_chunk->get_segment(ColumnID{1})));
}

}  // namespace hyrise
