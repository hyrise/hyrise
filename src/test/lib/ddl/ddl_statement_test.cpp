#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "tasks/chunk_compression_task.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "types.hpp"
#include "hyrise.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"

namespace opossum {

class DDLStatementTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    // We reload table_a every time since it is modified during the test case.
    _table_a = load_table("resources/test_data/tbl/int_float_create_index_test.tbl", 2);
    ChunkEncoder::encode_all_chunks(_table_a);
    Hyrise::get().storage_manager.add_table("table_a", _table_a);
  }

  // Tables modified during test case
  std::shared_ptr<Table> _table_a;

  const std::string _create_index_single_column = "CREATE INDEX myindex ON table_a (a)";
  const std::string _create_index_multi_column = "CREATE INDEX myindex ON table_a (a, b)";
};

TEST_F(DDLStatementTest, CreateIndexSingleColumn) {
  auto sql_pipeline = SQLPipelineBuilder{_create_index_single_column}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");
  auto column_ids = std::make_shared<std::vector<ColumnID>>();
  column_ids->emplace_back(ColumnID{0});

  auto actual_index = targeted_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == "myindex");
  EXPECT_TRUE(actual_index.column_ids == *column_ids);

  auto chunk_count = targeted_table->chunk_count();
  for(ChunkID id=ChunkID{0}; id < chunk_count; id+=1) {
    auto current_chunk = targeted_table->get_chunk(id);
    auto actual_indices = current_chunk->get_indexes(*column_ids);
    EXPECT_TRUE(actual_indices.size() == 1);
  }
}

TEST_F(DDLStatementTest, CreateIndexMultiColumn) {
  auto sql_pipeline = SQLPipelineBuilder{_create_index_multi_column}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");
  auto column_ids = std::make_shared<std::vector<ColumnID>>();
  column_ids->emplace_back(ColumnID{0});
  column_ids->emplace_back(ColumnID{1});

  auto actual_index = targeted_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == "myindex");
  EXPECT_TRUE(actual_index.column_ids == *column_ids);

  auto chunk_count = targeted_table->chunk_count();
  for(ChunkID id=ChunkID{0}; id < chunk_count; id+=1) {
    auto current_chunk = targeted_table->get_chunk(id);
    auto applied_indices = current_chunk->get_indexes(*column_ids);
    EXPECT_TRUE(applied_indices.size() == 1);
  }
}

TEST_F(DDLStatementTest, CreateIndexWithoutName) {
  auto sql_pipeline = SQLPipelineBuilder{"CREATE INDEX ON table_a (a)"}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");
  auto column_ids = std::make_shared<std::vector<ColumnID>>();
  column_ids->emplace_back(ColumnID{0});

  auto actual_index = targeted_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == "table_a_a");
  EXPECT_TRUE(actual_index.column_ids == *column_ids);

  auto chunk_count = targeted_table->chunk_count();
  for(ChunkID id=ChunkID{0}; id < chunk_count; id+=1) {
    auto current_chunk = targeted_table->get_chunk(id);
    auto applied_indices = current_chunk->get_indexes(*column_ids);
    EXPECT_TRUE(applied_indices.size() == 1);
  }
}

TEST_F(DDLStatementTest, CreateIndexIfNotExistsRun) {
  auto sql_pipeline = SQLPipelineBuilder{"CREATE INDEX myindex IF NOT EXISTS ON table_a (a)"}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");
  auto column_ids = std::make_shared<std::vector<ColumnID>>();
  column_ids->emplace_back(ColumnID{0});

  auto actual_index = targeted_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == "myindex");
  EXPECT_TRUE(actual_index.column_ids == *column_ids);

  auto chunk_count = targeted_table->chunk_count();
  for(ChunkID id=ChunkID{0}; id < chunk_count; id+=1) {
    auto current_chunk = targeted_table->get_chunk(id);
    auto actual_indices = current_chunk->get_indexes(*column_ids);
    EXPECT_TRUE(actual_indices.size() == 1);
  }
}

TEST_F(DDLStatementTest, CreateIndexIfNotExistsAbort) {
  // auto sql_pipeline = SQLPipelineBuilder{_create_index_single_column}.create_pipeline();

  //const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  //EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  //auto second_sql_pipeline = SQLPipelineBuilder{"CREATE INDEX myindex IF NOT EXISTS ON table_a (a, b)"}.create_pipeline();

  // This seems to be a very generic test, which could fail for various reasons, however a failing 'Assert' statement
  // leads to an std::logic_error apparently, so this is the way
  //EXPECT_THROW(second_sql_pipeline.get_result_table(), std::logic_error);
}

}