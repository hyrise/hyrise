#include "../base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

class MetaTableManagerTest : public BaseTest {};

TEST_F(MetaTableManagerTest, TableBasedMetaData) {
  // This tests a bunch of meta tables that are somehow related to the tables stored in the StorageManager.
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto prefix = MetaTableManager::META_PREFIX;
  const auto path = std::string{"resources/test_data/tbl/meta_tables/meta_"};
  for (const auto& meta_table_name : {"tables", "columns", "chunks", "segments", "segments_accurate"}) {
    SCOPED_TRACE(meta_table_name);

    const auto int_int = load_table("resources/test_data/tbl/int_int.tbl", 2);
    const auto int_int_int_null = load_table("resources/test_data/tbl/int_int_int_null.tbl", 100);
    ChunkEncoder::encode_chunk(int_int_int_null->get_chunk(ChunkID{0}), int_int_int_null->column_data_types(),
                               {{EncodingType::RunLength},
                                {EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                                {EncodingType::Unencoded}});

    if (storage_manager.has_table("int_int")) storage_manager.drop_table("int_int");
    if (storage_manager.has_table("int_int_int_null")) storage_manager.drop_table("int_int_int_null");

    storage_manager.add_table("int_int", int_int);
    storage_manager.add_table("int_int_int_null", int_int_int_null);

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + ".tbl");
      EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
    }

    // Update the tables
    SQLPipelineBuilder{"UPDATE int_int SET a = a + 1000 WHERE a < 1000"}.create_pipeline().get_result_table();
    SQLPipelineBuilder{"INSERT INTO int_int_int_null (a, b, c) VALUES (NULL, 1, 2)"}
        .create_pipeline()
        .get_result_table();

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + "_updated.tbl");
      EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
    }
  }

  // Test meta table for chunk sort orders
  {
    const auto int_int = load_table("resources/test_data/tbl/int_int.tbl", 100);
    storage_manager.add_table("int_int_sort", int_int);

    const auto meta_table = storage_manager.get_table(prefix + "chunk_sort_orders");
    EXPECT_EQ(meta_table->row_count(), 0ul);

    storage_manager.get_table("int_int_sort")
        ->get_chunk(ChunkID{0})
        ->set_ordered_by({ColumnID{1}, OrderByMode::Ascending});
    const auto meta_table_updated = storage_manager.get_table(prefix + "chunk_sort_orders");
    EXPECT_EQ(meta_table_updated->row_count(), 1ul);
    EXPECT_EQ(meta_table_updated->get_value<int32_t>("chunk_id", 0), 0);
    EXPECT_EQ(meta_table_updated->get_value<int32_t>("column_id", 0), 1);
    EXPECT_EQ(meta_table_updated->get_value<pmr_string>("order_mode", 0), pmr_string{"AscendingNullsFirst"});
  }

  {
    // TEST SQL features on meta tables
    const auto result = SQLPipelineBuilder{"SELECT COUNT(*) FROM meta_tables WHERE table_name = 'int_int'"}
                            .create_pipeline()
                            .get_result_table();

    EXPECT_EQ(result.first, SQLPipelineStatus::Success);
    EXPECT_EQ(result.second->get_value<int64_t>(ColumnID{0}, 0), 1);
  }
}

}  // namespace opossum
