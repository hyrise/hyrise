#include "base_test.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"

namespace hyrise {

// General behavior of MetaSegmentsAccurateTable is tested in meta_table_test.cpp.
class MetaSegmentsAccurateTest : public BaseTest {
 protected:
  std::shared_ptr<Table> int_int;

  void SetUp() override {
    int_int = load_table("resources/test_data/tbl/int_int.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("int_int", int_int);
  }
};

TEST_F(MetaSegmentsAccurateTest, UsesDistinctCountStatistics) {
  resolve_data_type(DataType::Int, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    const auto attribute_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
    attribute_statistics->set_statistics_object(std::make_shared<DistinctValueCount>(1234));
    auto mock_pruning_statistics = ChunkPruningStatistics{attribute_statistics, attribute_statistics};

    int_int->get_chunk(ChunkID{0})->set_pruning_statistics(mock_pruning_statistics);
    int_int->get_chunk(ChunkID{1})->set_pruning_statistics(mock_pruning_statistics);
  });

  const auto expected_table = Table::create_dummy_table({{"distinct_value_count", DataType::Long, false}});
  expected_table->append({int64_t{1234}});
  expected_table->append({int64_t{1234}});
  expected_table->append({int64_t{1234}});
  expected_table->append({int64_t{1234}});

  const auto& result_table = SQLPipelineBuilder{"SELECT distinct_value_count FROM meta_segments_accurate"}
                                 .create_pipeline()
                                 .get_result_table()
                                 .second;
  EXPECT_TABLE_EQ_UNORDERED(result_table, expected_table);
}

TEST_F(MetaSegmentsAccurateTest, FallBackValueAccess) {
  int_int->get_chunk(ChunkID{0})->set_pruning_statistics({});
  int_int->get_chunk(ChunkID{1})->set_pruning_statistics({});

  const auto expected_table = Table::create_dummy_table({{"table_name", DataType::String, false},
                                                         {"column_name", DataType::String, false},
                                                         {"chunk_id", DataType::Int, false},
                                                         {"distinct_value_count", DataType::Long, false}});
  expected_table->append({pmr_string{"int_int"}, pmr_string{"a"}, int32_t{0}, int64_t{2}});
  expected_table->append({pmr_string{"int_int"}, pmr_string{"a"}, int32_t{1}, int64_t{1}});
  expected_table->append({pmr_string{"int_int"}, pmr_string{"b"}, int32_t{0}, int64_t{2}});
  expected_table->append({pmr_string{"int_int"}, pmr_string{"b"}, int32_t{1}, int64_t{1}});

  {
    // Case 1: No pruning statistics set.
    int_int->get_chunk(ChunkID{0})->set_pruning_statistics({});
    int_int->get_chunk(ChunkID{1})->set_pruning_statistics({});

    const auto& result_table =
        SQLPipelineBuilder{"SELECT table_name, column_name, chunk_id, distinct_value_count FROM meta_segments_accurate"}
            .create_pipeline()
            .get_result_table()
            .second;
    EXPECT_TABLE_EQ_UNORDERED(result_table, expected_table);
  }

  {
    // Case 2: Pruning statistics without distinct value count.
    int_int->get_chunk(ChunkID{0})->set_pruning_statistics(ChunkPruningStatistics{2});
    int_int->get_chunk(ChunkID{1})->set_pruning_statistics(ChunkPruningStatistics{2});

    const auto& result_table =
        SQLPipelineBuilder{"SELECT table_name, column_name, chunk_id, distinct_value_count FROM meta_segments_accurate"}
            .create_pipeline()
            .get_result_table()
            .second;
    EXPECT_TABLE_EQ_UNORDERED(result_table, expected_table);
  }
}

}  // namespace hyrise
