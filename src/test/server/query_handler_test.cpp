#include "base_test.hpp"
#include "gtest/gtest.h"

#include "server/query_handler.hpp"

namespace opossum {

class QueryHandlerTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto& _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", _table_a);
  }
};

TEST_F(QueryHandlerTest, ExecutePipeline) {
  const std::string query = "SELECT 1;";
  const auto& result = QueryHandler::execute_pipeline(query, true);

  EXPECT_TRUE(result.error.empty());
  EXPECT_EQ(result.result_table->column_count(), 1);
  EXPECT_EQ(result.result_table->row_count(), 1);
  EXPECT_PRED_FORMAT2(testing::IsSubstring, "Execution info:", result.execution_information);
  EXPECT_EQ(result.root_operator, OperatorType::Projection);
}

TEST_F(QueryHandlerTest, ExecutePipelineInvalidStatement) {
  const std::string query = "SELECT * FROM;";
  const auto& result = QueryHandler::execute_pipeline(query, true);

  EXPECT_FALSE(result.error.empty());
  EXPECT_FALSE(result.result_table);
  EXPECT_TRUE(result.execution_information.empty());
}

TEST_F(QueryHandlerTest, CreatePreparedPlan) {
  const auto& error = QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");

  EXPECT_FALSE(error.has_value());
  EXPECT_TRUE(Hyrise::get().storage_manager.has_prepared_plan("test_statement"));
}

TEST_F(QueryHandlerTest, CreateInvalidPreparedPlan) {
  // Error: table does not exist
  const auto& error1 = QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM non_existent WHERE a > ?");

  EXPECT_TRUE(error1.has_value());
  EXPECT_PRED_FORMAT2(testing::IsSubstring, "Did not find a table", error1.value());
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan("test_statement"));

  // Error: Invalid SQL statement
  const auto& error2 = QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM WHERE a > ?");

  EXPECT_TRUE(error2.has_value());
  EXPECT_PRED_FORMAT2(testing::IsSubstring, "SQL query not valid", error2.value());

  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan("test_statement"));
}

TEST_F(QueryHandlerTest, BindParameters) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {123}};

  const auto result = QueryHandler::bind_prepared_plan(specification);
  EXPECT_TRUE(std::holds_alternative<std::shared_ptr<AbstractOperator>>(result));
  EXPECT_EQ(std::get<1>(result)->type(), OperatorType::TableScan);
}

TEST_F(QueryHandlerTest, BindInvalidParameters) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  // Error: Wrong number of parameters
  const auto specification = PreparedStatementDetails{"test_statement", "", {123, 12}};

  const auto result = QueryHandler::bind_prepared_plan(specification);
  EXPECT_TRUE(std::holds_alternative<std::string>(result));
  EXPECT_PRED_FORMAT2(testing::IsSubstring, "Incorrect number of parameters supplied", std::get<0>(result));
}

TEST_F(QueryHandlerTest, ExecutePreparedStatement) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {123}};
  const auto pqp = std::get<1>(QueryHandler::bind_prepared_plan(specification));

  auto transaction_context = QueryHandler::get_new_transaction_context();
  pqp->set_transaction_context_recursively(transaction_context);

  const auto& result_table = QueryHandler::execute_prepared_statement(pqp);
  EXPECT_EQ(result_table->row_count(), 2u);
  EXPECT_EQ(result_table->column_count(), 2u);
}

TEST_F(QueryHandlerTest, CorrectlyInvalidateStatements) {
  QueryHandler::setup_prepared_plan("", "SELECT * FROM table_a WHERE a > ?");
  const auto old_plan = Hyrise::get().storage_manager.get_prepared_plan("");

  // New unnamed statement invalidates existing prepared plan
  QueryHandler::setup_prepared_plan("", "SELECT * FROM table_a WHERE b > ?");
  const auto new_plan = Hyrise::get().storage_manager.get_prepared_plan("");

  EXPECT_NE(old_plan->hash(), new_plan->hash());

  // Simple queries invalidate an existing plan as well
  const std::string query = "SELECT 1;";
  QueryHandler::execute_pipeline(query, true);

  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(""));
}

}  // namespace opossum
