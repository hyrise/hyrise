#include "base_test.hpp"

#include "server/query_handler.hpp"

namespace opossum {

class TransactionHandlingTest : public BaseTest {
 protected:
  void SetUp() override {}
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
  const std::string query =
      "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1); "
      "BEGIN; INSERT INTO users(id) VALUES (2); "
      "ROLLBACK; SELECT * FROM users;";

  const auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();

  auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  // rollback transaction statement is executed successfully
  // in this case the second insert into the table gets rolled back
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table->column_count(), 1);
}

}  // namespace opossum
