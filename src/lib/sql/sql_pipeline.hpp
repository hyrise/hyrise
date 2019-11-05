#pragma once

#include <memory>

#include "SQLParserResult.h"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

// Holds relevant information about the execution of an SQLPipeline.
struct SQLPipelineMetrics {
  std::vector<std::shared_ptr<const SQLPipelineStatementMetrics>> statement_metrics;

  // This is different from the other measured times as we only get this for all statements at once
  std::chrono::nanoseconds parse_time_nanos{0};
};

std::ostream& operator<<(std::ostream& stream, const SQLPipelineMetrics& metrics);

/**
 * The SQLPipeline represents the flow from the basic SQL string (containing one or more statements) to the result
 * table(s) with all intermediate steps. Use the SQLPipelineBuilder to construct it.
 *
 * The SQLPipeline splits a given SQL string into its single SQL statements and wraps each statement in an
 * SQLPipelineStatement.
 */
class SQLPipeline : public Noncopyable {
 public:
  // Prefer using the SQLPipelineBuilder interface for constructing SQLPipelines conveniently
  SQLPipeline(const std::string& sql, const std::shared_ptr<TransactionContext>& transaction_context,
              const UseMvcc use_mvcc, const std::shared_ptr<Optimizer>& optimizer,
              const std::shared_ptr<SQLPhysicalPlanCache>& pqp_cache,
              const std::shared_ptr<SQLLogicalPlanCache>& lqp_cache, const CleanupTemporaries cleanup_temporaries);

  // Returns the original SQL string
  const std::string& get_sql() const;

  // Returns the SQL string for each statement.
  const std::vector<std::string>& get_sql_per_statement();

  // Returns the parsed SQL string for each statement.
  const std::vector<std::shared_ptr<hsql::SQLParserResult>>& get_parsed_sql_statements();

  // Returns the unoptimized LQP root for each statement.
  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_unoptimized_logical_plans();

  // Returns the optimized LQP root for each statement
  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_optimized_logical_plans();

  // Returns the Physical Plans for each statement.
  // The plans are either retrieved from the SQLPhysicalPlanCache or, if unavailable, translated from the optimized LQPs
  const std::vector<std::shared_ptr<AbstractOperator>>& get_physical_plans();

  // Returns all tasks for each statement that need to be executed for this query.
  const std::vector<std::vector<std::shared_ptr<OperatorTask>>>& get_tasks();

  // Executes all tasks, waits for them to finish, and returns
  //   - {Success, tables}     if the statement was successful
  //   - {RolledBack, tables}  if the transaction failed. There might be tables included if no explicit transaction
  //                           context was provided and statements auto-committed
  // The transaction status is somewhat redundant, as it could also be retrieved from the transaction_context. We
  // explicitly return it as part of get_result_table(s) to force the caller to take the possibility of a failed
  // transaction into account.
  //
  // If the pipeline is an xvalue, provide a variant that returns non-reference tables:
  //   const auto result = SQLPipelineBuilder{"SELECT ..."}.create_pipeline().get_result_tables()
  std::pair<SQLPipelineStatus, const std::vector<std::shared_ptr<const Table>>&> get_result_tables() &;
  std::pair<SQLPipelineStatus, std::vector<std::shared_ptr<const Table>>> get_result_tables() &&;

  // Shorthand for `get_result_tables().back()`
  std::pair<SQLPipelineStatus, const std::shared_ptr<const Table>&> get_result_table() &;
  std::pair<SQLPipelineStatus, std::shared_ptr<const Table>> get_result_table() &&;

  // Returns the TransactionContext that was passed to the SQLPipelineStatement, or nullptr if none was passed in.
  std::shared_ptr<TransactionContext> transaction_context() const;

  // This returns the SQLPipelineStatement that aborted the transaction, if any
  std::shared_ptr<SQLPipelineStatement> failed_pipeline_statement() const;

  size_t statement_count() const;

  // Returns whether the pipeline requires execution to handle all statements (e.g, one statement creates a Table,
  // another uses it)
  bool requires_execution() const;

  SQLPipelineMetrics& metrics();

  const std::shared_ptr<SQLPhysicalPlanCache> pqp_cache;
  const std::shared_ptr<SQLLogicalPlanCache> lqp_cache;

 private:
  std::string _sql;

  std::vector<std::shared_ptr<SQLPipelineStatement>> _sql_pipeline_statements;

  const std::shared_ptr<TransactionContext> _transaction_context;
  const std::shared_ptr<Optimizer> _optimizer;

  // Execution results
  std::vector<std::string> _sql_strings;
  std::vector<std::shared_ptr<hsql::SQLParserResult>> _parsed_sql_statements;
  std::vector<std::shared_ptr<AbstractLQPNode>> _unoptimized_logical_plans;
  std::vector<std::shared_ptr<AbstractLQPNode>> _optimized_logical_plans;
  std::vector<std::shared_ptr<AbstractOperator>> _physical_plans;
  std::vector<std::vector<std::shared_ptr<OperatorTask>>> _tasks;
  std::vector<std::shared_ptr<const Table>> _result_tables;

  // Indicates whether get_result_table() has been run yet and whether the execution was successful
  SQLPipelineStatus _pipeline_status{SQLPipelineStatus::NotExecuted};

  // Indicates whether translating a statement in the pipeline requires the execution of a previous statement
  // e.g. CREATE VIEW foo AS SELECT * FROM bar; SELECT * FROM foo;
  // --> requires execution of first statement before the second one can be translated
  bool _requires_execution{false};

  SQLPipelineMetrics _metrics{};

  std::shared_ptr<SQLPipelineStatement> _failed_pipeline_statement;
};

}  // namespace opossum
