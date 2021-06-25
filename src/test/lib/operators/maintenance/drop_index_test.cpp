
#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

#include "operators/maintenance/drop_index.hpp"
#include "operators/maintenance/create_index.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"
#include "tasks/chunk_compression_task.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

class DropIndexTest: public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table("TestTable", test_table);
    dummy_table_wrapper = std::make_shared<TableWrapper>(test_table);
    dummy_table_wrapper->execute();

    column_ids->emplace_back(ColumnID{static_cast<ColumnID>(test_table->column_id_by_name("b"))});

    create_index = std::make_shared<CreateIndex>(index_name, column_ids, true, table_name, dummy_table_wrapper);

    auto compression_task_0 = std::make_shared<ChunkCompressionTask>("TestTable", ChunkID{0});
    auto compression_task_1 = std::make_shared<ChunkCompressionTask>("TestTable", ChunkID{1});

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task_0, compression_task_1});

    const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    create_index->set_transaction_context(context);

    create_index->execute();
    context->commit();
  }

  std::shared_ptr<TableWrapper> dummy_table_wrapper;
  std::shared_ptr<Table> test_table;
  std::shared_ptr<CreateIndex> create_index;
  std::string index_name = "TestIndex";
  std::shared_ptr<std::vector<ColumnID>> column_ids = std::make_shared<std::vector<ColumnID>>();
  SegmentIndexType index_type;
  std::string table_name = "TestTable";
};

TEST_F(DropIndexTest, IndexStatisticsEmpty) {
  EXPECT_TRUE(test_table->indexes_statistics().size() == 1);
  auto table_wrapper = std::make_shared<TableWrapper>(test_table);
  table_wrapper->execute();
  auto drop_index = std::make_shared<DropIndex>(index_name, table_wrapper);

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  drop_index->set_transaction_context(context);

  drop_index->execute();
  context->commit();
  EXPECT_TRUE(test_table->indexes_statistics().size() == 0);
}

TEST_F(DropIndexTest, FailOnWrongIndexName) {
  EXPECT_TRUE(test_table->indexes_statistics().size() == 1);
  auto table_wrapper = std::make_shared<TableWrapper>(test_table);
  table_wrapper->execute();
  auto drop_index = std::make_shared<DropIndex>("WrongIndexName", table_wrapper);

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  drop_index->set_transaction_context(context);

  EXPECT_THROW(drop_index->execute(), std::logic_error);
  context->rollback(RollbackReason::Conflict);
}
}  // namespace opossum
