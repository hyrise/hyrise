#include "base_test.hpp"

#include "server/query_handler.hpp"

namespace opossum {

class TransactionHandlingTest : public BaseTest {
 protected:
  void SetUp() override {    
  }
};

TEST_F(TransactionHandlingTest, CreateTableWithinTransaction) {
  const std::string query = "BEGIN; CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1); COMMIT;";

  const auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();

  auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  // begin and commit transaction statements are executed successfully
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table, nullptr);  
  EXPECT_EQ(execution_information.root_operator, OperatorType::CommitTransaction);
}

TEST_F(TransactionHandlingTest, RollbackTransaction) {
  const std::string query = "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1); BEGIN; INSERT INTO users(id) VALUES (2); ROLLBACK; SELECT * FROM users;";

  const auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();

  auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  // rollback transaction statement (the second insert into the table gets rolled back in this case) is executed successfully
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table->column_count(), 1);
}

/*
TEST_F(QueryHandlerTest, CreatePreparedPlan) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");

  EXPECT_TRUE(Hyrise::get().storage_manager.has_prepared_plan("test_statement"));
}

TEST_F(QueryHandlerTest, BindParameters) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {123}};

  const auto result = QueryHandler::bind_prepared_plan(specification);
  EXPECT_EQ(result->type(), OperatorType::TableScan);
}

TEST_F(QueryHandlerTest, ExecutePreparedStatement) {
  QueryHandler::setup_prepared_plan("test_statement", "SELECT * FROM table_a WHERE a > ?");
  const auto specification = PreparedStatementDetails{"test_statement", "", {123}};
  const auto pqp = QueryHandler::bind_prepared_plan(specification);

  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
  pqp->set_transaction_context_recursively(transaction_context);

  const auto& result_table = QueryHandler::execute_prepared_plan(pqp);
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
  auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();
  QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan(""));
}*/

}  // namespace opossum
