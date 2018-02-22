#pragma once

#include <memory>

#include "SQLParserResult.h"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This is the unified interface to handle SQL queries and related operations.
 * This should be preferred over using SQLPipelineStatement directly, unless you really know why you need it.
 *
 * The SQLPipeline represents the flow from the basic SQL string to the result table with all intermediate steps.
 *
 * The basic idea of the SQLPipeline is that is splits a given SQL string into its single SQL statements and wraps each
 * statement in an SQLPipelineStatement. If a single statement is present, it creates just one SQLPipelineStatement.
 * If N statements are present, it creates N SQLPipelineStatements.
 *
 * The advantage of using this over a self created vector of SQLPipelineStatements is that some sanity checks are
 * performed here, e.g. TransactionContexts and dependent statements (CREATE VIEW X followed by SELECT * FROM X)
 *
 * The SQLPipeline holds all results and only hands them out as const references. If the SQLPipeline goes out of scope
 * while the results are still needed, the result references are invalid (except maybe the result table).
 */
class SQLPipeline : public Noncopyable {
 public:
  // No explicit transaction context constructors
  explicit SQLPipeline(const std::string& sql, const UseMvcc use_mvcc = UseMvcc::Yes);

  SQLPipeline(const std::string& sql, const std::shared_ptr<Optimizer>& optimizer,
              const UseMvcc use_mvcc = UseMvcc::Yes);

  SQLPipeline(const std::string& sql, const PreparedStatementCache& prepared_statements,
              const UseMvcc use_mvcc = UseMvcc::Yes);

  SQLPipeline(const std::string& sql, const std::shared_ptr<Optimizer>& optimizer,
              const PreparedStatementCache& prepared_statements, const UseMvcc use_mvcc = UseMvcc::Yes);

  // Explicit transaction context constructors
  SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context);

  SQLPipeline(const std::string& sql, const std::shared_ptr<Optimizer>& optimizer,
              std::shared_ptr<TransactionContext> transaction_context);

  SQLPipeline(const std::string& sql, const PreparedStatementCache& prepared_statements,
              std::shared_ptr<TransactionContext> transaction_context);

  SQLPipeline(const std::string& sql, const std::shared_ptr<Optimizer>& optimizer,
              const PreparedStatementCache& prepared_statements,
              std::shared_ptr<TransactionContext> transaction_context);

  // Returns the SQL string for each statement.
  const std::vector<std::string>& get_sql_strings();

  // Returns the parsed SQL string for each statement.
  const std::vector<std::shared_ptr<hsql::SQLParserResult>>& get_parsed_sql_statements();

  // Returns the unoptimized LQP root for each statement.
  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_unoptimized_logical_plans();

  // Returns the optimized LQP root for each statement.
  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_optimized_logical_plans();

  // Returns the SQLQueryPlan for each statement.
  // For now, this always uses the optimized LQP.
  const std::vector<std::shared_ptr<SQLQueryPlan>>& get_query_plans();

  // Returns all tasks for each statement that need to be executed for this query.
  const std::vector<std::vector<std::shared_ptr<OperatorTask>>>& get_tasks();

  // Executes all tasks, waits for them to finish, and returns the resulting table of the last statement.
  const std::shared_ptr<const Table>& get_result_table();

  // Returns the TransactionContext that was passed to the SQLPipelineStatement, or nullptr if none was passed in.
  const std::shared_ptr<TransactionContext>& transaction_context() const;

  // This returns the SQLPipelineStatement that caused this pipeline to throw an error.
  // If there is no failed statement, this fails
  const std::shared_ptr<SQLPipelineStatement>& failed_pipeline_statement() const;

  // Returns the number of SQLPipelineStatements present in this pipeline
  size_t statement_count() const;

  // Returns whether the pipeline requires execution to handle all statements
  bool requires_execution() const;

  // Returns the entire compile time. Only possible to get this after all statements have been executed or if the
  // pipeline does not require previous execution to compile all statements.
  std::chrono::microseconds compile_time_microseconds();

  // Returns the entire execution time
  std::chrono::microseconds execution_time_microseconds();

 private:
  SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context, const UseMvcc use_mvcc,
              const std::shared_ptr<Optimizer>& optimizer, const PreparedStatementCache& prepared_statements);

  std::vector<std::shared_ptr<SQLPipelineStatement>> _sql_pipeline_statements;

  std::shared_ptr<TransactionContext> _transaction_context;
  std::shared_ptr<Optimizer> _optimizer;

  // Execution results
  std::vector<std::string> _sql_strings;
  std::vector<std::shared_ptr<hsql::SQLParserResult>> _parsed_sql_statements;
  std::vector<std::shared_ptr<AbstractLQPNode>> _unoptimized_logical_plans;
  std::vector<std::shared_ptr<AbstractLQPNode>> _optimized_logical_plans;
  std::vector<std::shared_ptr<SQLQueryPlan>> _query_plans;
  std::vector<std::vector<std::shared_ptr<OperatorTask>>> _tasks;
  std::shared_ptr<const Table> _result_table;
  // Indicates whether get_result_table has been run successfully
  bool _pipeline_was_executed = false;

  // Indicates whether translating a statement in the pipeline requires the execution of a previous statement
  // e.g. CREATE VIEW foo AS SELECT * FROM bar; SELECT * FROM foo;
  // --> requires execution of first statement before the second one can be translated
  bool _requires_execution;

  std::shared_ptr<SQLPipelineStatement> _failed_pipeline_statement;

  // Execution times
  std::chrono::microseconds _compile_time_microseconds{};
  std::chrono::microseconds _execution_time_microseconds{};
};

}  // namespace opossum
