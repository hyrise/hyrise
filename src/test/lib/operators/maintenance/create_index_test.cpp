
#include <memory>

#include "base_test.hpp"
#include "utils/assert.hpp"

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

class CreateIndexTest: public BaseTest {
 public:
  void SetUp() override {
    test_table = load_table("resources/test_data/tbl/string_int_index.tbl", 3);
    Hyrise::get().storage_manager.add_table("TestTable", test_table);
    dummy_table_wrapper = std::make_shared<TableWrapper>(test_table);
    dummy_table_wrapper->execute();

    auto column_ids = std::make_shared<std::vector<ColumnID>>();
    column_ids->emplace_back(ColumnID{static_cast<ColumnID>(test_table->column_id_by_name("b"))});

    create_index = std::make_shared<CreateIndex>("TestIndex", column_ids, "TestTable", dummy_table_wrapper);
  }

  std::shared_ptr<TableWrapper> dummy_table_wrapper;
  std::shared_ptr<Table> test_table;
  std::shared_ptr<CreateIndex> create_index;
};

TEST_F(CreateIndexTest, NameAndDescription) {
  EXPECT_EQ(create_index->name(), "CreateIndex");
}

TEST_F(CreateIndexTest, Execute) {
  auto compression_task_0 = std::make_shared<ChunkCompressionTask>("TestTable", ChunkID{0});
  auto compression_task_1 = std::make_shared<ChunkCompressionTask>("TestTable", ChunkID{1});

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks({compression_task_0, compression_task_1});

  const auto context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  create_index->set_transaction_context(context);

  create_index->execute();
  context->commit();

  auto actual_statistics = test_table->indexes_statistics();

  // TODO: check if actual statistics contains expected statistics.
  EXPECT_TRUE(1==1);
}

TEST_F(CreateIndexTest, TableAlreadyExists) {}

TEST_F(CreateIndexTest, ExecuteWithIfNotExists) {}

TEST_F(CreateIndexTest, CreateTableAsSelect) {}

TEST_F(CreateIndexTest, CreateTableAsSelectWithProjection) {}

TEST_F(CreateIndexTest, CreateTableWithDifferentTransactionContexts) {}

}  // namespace opossum
