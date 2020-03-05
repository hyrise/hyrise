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
  EXPECT_EQ(execution_information.custom_command_complete_message.value(), "COMMIT");
}

TEST_F(TransactionHandlingTest, RollbackTransaction) {
  const std::string query =
      "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1);"
      "INSERT INTO users(id) VALUES (2);"
      "BEGIN; INSERT INTO users(id) VALUES (3);"
      "ROLLBACK; SELECT * FROM users;";

  const auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();

  auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  // rollback transaction statement is executed successfully
  // in this case the second insert into the table gets rolled back
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table->row_count(), 2);
}

// Testing whether the TransactionContext changes its phase correctly
// and whether the auto-commit property is set correctly in different scenarios
TEST_F(TransactionHandlingTest, TestTransactionContextInternals) {
  std::string query = "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1);";

  auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context();

  auto execution_info_transaction_context_pair =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

  auto execution_information = execution_info_transaction_context_pair.first;

  // normally, when user has not begun a transaction yet, the transaction context is in "auto-commit" mode
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(transaction_ctx->is_auto_commit(), true);
  EXPECT_EQ(transaction_ctx->phase(), TransactionPhase::Committed);

  transaction_ctx = execution_info_transaction_context_pair.second;

  query = "BEGIN; INSERT INTO users(id) VALUES (2);";

  execution_info_transaction_context_pair =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
  execution_information = execution_info_transaction_context_pair.first;
  transaction_ctx = execution_info_transaction_context_pair.second;

  // when the user begins a transaction, a new transaction context is created internally (not in "auto-commit" mode)
  // the transaction is therefore still active until the user either rolls back or commits
  EXPECT_EQ(transaction_ctx->phase(), TransactionPhase::Active);
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(transaction_ctx->is_auto_commit(), false);

  query = "ROLLBACK;";

  execution_info_transaction_context_pair =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
  execution_information = execution_info_transaction_context_pair.first;

  // now that the user rolled back,
  // the transaction context is in the successful state of having been rolled back on purpose
  EXPECT_EQ(transaction_ctx->phase(), TransactionPhase::RolledBackByUser);
  EXPECT_TRUE(execution_information.error_message.empty());

  // internally the transaction context returned by the pipeline is nullptr
  // in order to force creating a new one in the next pipeline execution
  transaction_ctx = execution_info_transaction_context_pair.second;
  EXPECT_EQ(transaction_ctx, nullptr);
}

}  // namespace opossum
