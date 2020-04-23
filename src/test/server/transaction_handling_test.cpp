#include "base_test.hpp"

#include "server/query_handler.hpp"

namespace opossum {

class TransactionHandlingTest : public BaseTest {};

TEST_F(TransactionHandlingTest, CreateTableWithinTransaction) {
  const std::string query = "BEGIN; CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1); COMMIT;";

  auto [execution_information, transaction_ctx] =
      QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr);

  // begin and commit transaction statements are executed successfully
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table, nullptr);
  EXPECT_EQ(execution_information.custom_command_complete_message.value(), "COMMIT");
  EXPECT_EQ(Hyrise::get().storage_manager.get_table("users")->row_count(), 1);
}

TEST_F(TransactionHandlingTest, RollbackTransaction) {
  const std::string query =
      "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1);"
      "INSERT INTO users(id) VALUES (2);"
      "BEGIN; INSERT INTO users(id) VALUES (3);"
      "ROLLBACK; SELECT * FROM users;";

  auto [execution_information, _] = QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr);

  // rollback transaction statement is executed successfully
  // in this case the second insert into the table gets rolled back
  EXPECT_TRUE(execution_information.error_message.empty());
  EXPECT_EQ(execution_information.result_table->row_count(), 2);
}

// Testing whether the TransactionContext changes its phase correctly
// and whether the auto-commit property is set correctly in different scenarios
TEST_F(TransactionHandlingTest, TestTransactionContextInternals) {
  auto transaction_ctx = std::shared_ptr<TransactionContext>{};

  {
    std::string query = "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1);";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

    auto execution_information = execution_info_transaction_context_pair.first;

    // The transaction context should be in "auto-commit" mode
    EXPECT_TRUE(execution_information.error_message.empty());
    EXPECT_EQ(transaction_ctx, nullptr);

    transaction_ctx = execution_info_transaction_context_pair.second;
  }

  {
    std::string query = "BEGIN; INSERT INTO users(id) VALUES (2);";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
    auto execution_information = execution_info_transaction_context_pair.first;
    transaction_ctx = execution_info_transaction_context_pair.second;

    // when the user begins a transaction, a new transaction context is created internally (not in "auto-commit" mode)
    // the transaction is therefore still active until the user either rolls back or commits
    EXPECT_EQ(transaction_ctx->phase(), TransactionPhase::Active);
    EXPECT_TRUE(execution_information.error_message.empty());
    EXPECT_EQ(transaction_ctx->is_auto_commit(), false);
  }

  {
    std::string query = "ROLLBACK;";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
    auto execution_information = execution_info_transaction_context_pair.first;

    // now that the user rolled back,
    // the transaction context is in the successful state of having been rolled back on purpose
    EXPECT_EQ(transaction_ctx->phase(), TransactionPhase::RolledBackByUser);
    EXPECT_TRUE(execution_information.error_message.empty());

    // internally the transaction context returned by the pipeline is nullptr
    // in order to force creating a new one in the next pipeline execution
    transaction_ctx = execution_info_transaction_context_pair.second;
    EXPECT_EQ(transaction_ctx, nullptr);
  }
}

// unit test ensuring that transactions don't see the changes of other uncommitted transactions
// read more about visibility of transaction changes on the MVCC wiki page
TEST_F(TransactionHandlingTest, TestTransactionSideEffects) {
  auto transaction_ctx = std::shared_ptr<TransactionContext>{};

  {
    std::string query = "CREATE TABLE users (id INT); INSERT INTO users(id) VALUES (1);";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);

    auto execution_information = execution_info_transaction_context_pair.first;
    EXPECT_TRUE(execution_information.error_message.empty());

    transaction_ctx = execution_info_transaction_context_pair.second;
  }

  {
    // user A wants to delete a row from the table and starts transaction
    std::string query = "BEGIN; DELETE FROM users WHERE id = 1;";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
    auto execution_information = execution_info_transaction_context_pair.first;
    EXPECT_TRUE(execution_information.error_message.empty());

    transaction_ctx = execution_info_transaction_context_pair.second;
  }

  {
    // meanwhile user B checks all elements in that table
    std::string query_2 = "SELECT * FROM users;";

    auto execution_info_transaction_context_pair_2 =
        QueryHandler::execute_pipeline(query_2, SendExecutionInfo::Yes, nullptr);

    auto execution_information_2 = execution_info_transaction_context_pair_2.first;

    // the DELETE statement from user A has not been committed yet
    // that's why the change is not visible yet
    EXPECT_TRUE(execution_information_2.error_message.empty());
    EXPECT_EQ(execution_information_2.result_table->row_count(), 1);
  }

  {
    // user A finally commits the transaction
    std::string query = "COMMIT;";

    auto execution_info_transaction_context_pair =
        QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx);
    auto execution_information = execution_info_transaction_context_pair.first;
    EXPECT_TRUE(execution_information.error_message.empty());
  }

  {
    // user B checks all elements in that table again
    std::string query_2 = "SELECT * FROM users;";

    auto execution_info_transaction_context_pair_2 =
        QueryHandler::execute_pipeline(query_2, SendExecutionInfo::Yes, nullptr);

    auto execution_information_2 = execution_info_transaction_context_pair_2.first;

    // now the changes of the DELETE statement are visible
    EXPECT_TRUE(execution_information_2.error_message.empty());
    EXPECT_EQ(execution_information_2.result_table->row_count(), 0);
  }
}

TEST_F(TransactionHandlingTest, InvalidTransactionTransitions) {
  {
    auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    std::string query = "BEGIN";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx), InvalidInputException);
  }

  {
    auto transaction_ctx = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    std::string query = "COMMIT; COMMIT";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, transaction_ctx), InvalidInputException);
  }

  {
    std::string query = "BEGIN; BEGIN";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr), InvalidInputException);
  }

  {
    std::string query = "COMMIT";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr), InvalidInputException);
  }

  {
    std::string query = "BEGIN; COMMIT; COMMIT";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr), InvalidInputException);
  }

  {
    std::string query = "BEGIN; ROLLBACK; COMMIT";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr), InvalidInputException);
  }

  {
    std::string query = "BEGIN; ROLLBACK; BEGIN; COMMIT; ROLLBACK;";
    EXPECT_THROW(QueryHandler::execute_pipeline(query, SendExecutionInfo::Yes, nullptr), InvalidInputException);
  }
}

}  // namespace opossum
