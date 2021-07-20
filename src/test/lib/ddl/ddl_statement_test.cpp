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
    Hyrise::get().storage_manager.add_table("table_a", _table_a);
    auto compression_task_0 = std::make_shared<ChunkCompressionTask>("table_a", ChunkID{0});
    auto compression_task_1 = std::make_shared<ChunkCompressionTask>("table_a", ChunkID{1});

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task_0, compression_task_1});
  }

  // Tables modified during test case
  std::shared_ptr<Table> _table_a;

  const std::string _create_index = "CREATE INDEX myindex ON table_a (a)";
  const std::string _alter_table = "ALTER TABLE table_a DROP COLUMN a";
};

TEST_F(DDLStatementTest, CreateIndex) {
  auto sql_pipeline = SQLPipelineBuilder{_create_index}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");
  auto column_ids = std::make_shared<std::vector<ColumnID>>();
  column_ids->emplace_back(ColumnID{0});

  auto actual_index = targeted_table->indexes_statistics().at(0);

  EXPECT_TRUE(actual_index.name == "myindex");
  EXPECT_TRUE(actual_index.column_ids == *column_ids);
}

TEST_F(DDLStatementTest, AlterTableDropColumn) {
  auto sql_pipeline = SQLPipelineBuilder{_alter_table}.create_pipeline();

  const auto& [pipeline_status, table] = sql_pipeline.get_result_table();
  EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);

  auto targeted_table = Hyrise::get().storage_manager.get_table("table_a");

  EXPECT_EQ(targeted_table->column_count(), 1u);
  EXPECT_EQ(targeted_table->column_name(ColumnID{0}), "b");
}

}