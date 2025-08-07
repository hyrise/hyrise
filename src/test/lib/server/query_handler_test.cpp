#include "base_test.hpp"
#include "operators/get_table.hpp"
#include "server/query_handler.hpp"
#include "storage/prepared_plan.hpp"

namespace hyrise {

class QueryHandlerTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto& table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    Hyrise::get().catalog.add_table("table_a", table_a);
  }
};

TEST_F(QueryHandlerTest, ExecutePipeline) {
  const auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline("SELECT 1;", SendExecutionInfo::Yes, nullptr);

  EXPECT_TRUE(execution_information.error_messages.empty());
  EXPECT_EQ(execution_information.result_table->column_count(), 1);
  EXPECT_EQ(execution_information.result_table->row_count(), 1);
  EXPECT_PRED_FORMAT2(testing::IsSubstring, "Execution info:", execution_information.pipeline_metrics);
  EXPECT_EQ(execution_information.root_operator_type, OperatorType::Projection);
}

TEST_F(QueryHandlerTest, CreatePreparedPlan) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");

  EXPECT_TRUE(Hyrise::get().catalog.has_prepared_plan("test_statement"));
}

TEST_F(QueryHandlerTest, BindParameters) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a = ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {12345}};

  const auto bound_plan = QueryHandler::bind_prepared_plan(specification);
  EXPECT_EQ(bound_plan->type(), OperatorType::Validate);

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(bound_plan->left_input()->left_input());
  ASSERT_TRUE(get_table);

  // Check that the optimizer was executed. We cannot distinguish an optimized PQP from an unoptimized PQP, so we check
  // whether the chunk pruning information was set in the GetTable operator. That would have been done by the
  // ChunkPruningRule, which could not have been successful before the bound value (12345) was known.
  ASSERT_FALSE(get_table->pruned_chunk_ids().empty());
}

TEST_F(QueryHandlerTest, ExecutePreparedStatement) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {123}};
  const auto pqp = QueryHandler::bind_prepared_plan(specification);

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  pqp->set_transaction_context_recursively(transaction_context);

  const auto& result_table = QueryHandler::execute_prepared_plan(pqp);
  EXPECT_EQ(result_table->row_count(), 2);
  EXPECT_EQ(result_table->column_count(), 2);
}

TEST_F(QueryHandlerTest, CorrectlyInvalidateStatements) {
  QueryHandler::setup_prepared_plan("", "SELECT * FROM table_a WHERE a > ?");
  const auto old_plan_id = Hyrise::get().catalog.prepared_plan_id("");
  EXPECT_NE(old_plan_id, INVALID_OBJECT_ID);
  const auto old_plan = Hyrise::get().storage_manager.get_prepared_plan(old_plan_id);

  // New unnamed statement invalidates existing prepared plan.
  QueryHandler::setup_prepared_plan("", "SELECT * FROM table_a WHERE b > ?");
  const auto new_plan_id = Hyrise::get().catalog.prepared_plan_id("");
  EXPECT_NE(new_plan_id, INVALID_OBJECT_ID);
  EXPECT_NE(new_plan_id, old_plan_id);
  const auto new_plan = Hyrise::get().storage_manager.get_prepared_plan(new_plan_id);

  EXPECT_NE(old_plan->hash(), new_plan->hash());

  // Simple queries invalidate an existing plan as well.
  QueryHandler::execute_pipeline("SELECT 1;", SendExecutionInfo::Yes, nullptr);

  EXPECT_FALSE(Hyrise::get().catalog.has_prepared_plan(""));
}

}  // namespace hyrise
