#pragma once

#include <string>

#include "SQLParserResult.h"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/table.hpp"

namespace opossum {

using PreparedStatementCache = SQLQueryCache<SQLQueryPlan>;

// Holds relevant information about the execution of an SQLPipelineStatement.
struct SQLPipelineStatementMetrics {
  std::chrono::nanoseconds translate_time_nanos{};
  std::chrono::nanoseconds optimize_time_nanos{};
  std::chrono::nanoseconds compile_time_nanos{};
  std::chrono::nanoseconds execution_time_nanos{};

  bool query_plan_cache_hit = false;
};

/**
 * This is the unified interface to handle SQL queries and related operations.
 * This should rarely be used directly - use SQLPipeline instead, as it creates the correct SQLPipelineStatement(s).
 *
 * The basic idea of the SQLPipelineStatement is that it represents the flow from a single SQL statement to the result
 * table with all intermediate steps. These intermediate steps call the previous step that is required. The intermediate
 * results are all cached so calling a method twice will return the already existing value.
 *
 * The SQLPipelineStatement holds all results and only hands them out as const references. If the SQLPipelineStatement
 * goes out of scope while the results are still needed, the result references are invalid (except maybe the
 * result table).
 *
 * E.g: calling sql_pipeline_statement.get_result_table() will result in the following "call stack"
 * get_result_table -> get_tasks -> get_query_plan -> get_optimized_logical_plan -> get_parsed_sql
 */
class SQLPipelineStatement : public Noncopyable {
 public:
  // Prefer using the SQLPipelineBuilder for constructing SQLPipelineStatements conveniently
  SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                       const UseMvcc use_mvcc, const std::shared_ptr<TransactionContext>& transaction_context,
                       const std::shared_ptr<LQPTranslator>& lqp_translator,
                       const std::shared_ptr<Optimizer>& optimizer,
                       const std::shared_ptr<PreparedStatementCache>& prepared_statements,
                       const CleanupTemporaries cleanup_temporaries);

  // Returns the raw SQL string.
  const std::string& get_sql_string();

  // Returns the parsed SQL string.
  const std::shared_ptr<hsql::SQLParserResult>& get_parsed_sql_statement();

  // Returns all unoptimized LQP roots.
  const std::shared_ptr<AbstractLQPNode>& get_unoptimized_logical_plan();

  // Returns all optimized LQP roots.
  const std::shared_ptr<AbstractLQPNode>& get_optimized_logical_plan();

  // For now, this always uses the optimized LQP.
  const std::shared_ptr<SQLQueryPlan>& get_query_plan();

  // Returns all task sets that need to be executed for this query.
  const std::vector<std::shared_ptr<OperatorTask>>& get_tasks();

  // Executes all tasks, waits for them to finish, and returns the resulting table.
  const std::shared_ptr<const Table>& get_result_table();

  // Returns the TransactionContext that was either passed to or created by the SQLPipelineStatement.
  // This can be a nullptr if no transaction management is wanted.
  const std::shared_ptr<TransactionContext>& transaction_context() const;

  const std::shared_ptr<SQLPipelineStatementMetrics>& metrics() const;

 private:
  const std::string _sql_string;
  const UseMvcc _use_mvcc;

  // Perform MVCC commit right after the Statement was executed
  const bool _auto_commit;

  // Might be the Statement's own transaction context, or the one shared by all Statements in a Pipeline
  std::shared_ptr<TransactionContext> _transaction_context;

  // The translator to be used to translate the abstract LQP into an executable PQP
  const std::shared_ptr<LQPTranslator> _lqp_translator;

  const std::shared_ptr<Optimizer> _optimizer;

  // Execution results
  std::shared_ptr<hsql::SQLParserResult> _parsed_sql_statement;
  std::shared_ptr<AbstractLQPNode> _unoptimized_logical_plan;
  std::shared_ptr<AbstractLQPNode> _optimized_logical_plan;
  std::shared_ptr<SQLQueryPlan> _query_plan;
  std::vector<std::shared_ptr<OperatorTask>> _tasks;
  std::shared_ptr<const Table> _result_table;
  // Assume there is an output table. Only change if nullptr is returned from execution.
  bool _query_has_output = true;

  std::shared_ptr<SQLPipelineStatementMetrics> _metrics;

  std::shared_ptr<PreparedStatementCache> _prepared_statements;
  std::unordered_map<ValuePlaceholderID, ParameterID> _parameter_ids;

  // Delete temporary tables
  const CleanupTemporaries _cleanup_temporaries;
};

}  // namespace opossum
