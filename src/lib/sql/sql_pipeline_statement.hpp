#pragma once

#include <string>

#include "SQLParserResult.h"
#include "cache/cache.hpp"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "storage/table.hpp"

namespace opossum {

// Holds relevant information about the execution of an SQLPipelineStatement.
struct SQLPipelineStatementMetrics {
  std::chrono::nanoseconds sql_translation_duration{};
  std::chrono::nanoseconds optimization_duration{};
  std::chrono::nanoseconds lqp_translation_duration{};
  std::chrono::nanoseconds plan_execution_duration{};

  bool query_plan_cache_hit = false;
};

/**
 * The SQLPipelineStatement represents the flow from a *single* SQL statement to the result table with all intermediate
 * steps. Don't construct this class directly, use the SQLPipelineBuilder instead
 *
 * Note:
 *  Calling get_result_table() will result in the following "call stack"
 *  get_result_table() -> get_tasks() -> get_pyhsical_plan_plan() -> get_optimized_logical_plan() ->
 *  get_unoptimized_logical_plan() -> get_parsed_sql()
 *
 * NOTE:
 *  If a physical plan for an SQL statement is in the SQLPhysicalPlanCache, it will be used instead of translating the optimized
 *  LQP (get_optimized_logical_plans()) into a PQP. Thus, in this case, the optimized LQP and PQP could be different.
 */
class SQLPipelineStatement : public Noncopyable {
 public:
  // Prefer using the SQLPipelineBuilder for constructing SQLPipelineStatements conveniently
  SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                       const UseMvcc use_mvcc, const std::shared_ptr<TransactionContext>& transaction_context,
                       const std::shared_ptr<LQPTranslator>& lqp_translator,
                       const std::shared_ptr<Optimizer>& optimizer, const CleanupTemporaries cleanup_temporaries);

  // Returns the raw SQL string.
  const std::string& get_sql_string();

  // Returns the parsed SQL string.
  const std::shared_ptr<hsql::SQLParserResult>& get_parsed_sql_statement();

  // Returns the unoptimized LQP for this statement.
  const std::shared_ptr<AbstractLQPNode>& get_unoptimized_logical_plan();

  // Returns the optimized LQP for this statement.
  const std::shared_ptr<AbstractLQPNode>& get_optimized_logical_plan();

  // Returns the PQP for this statement.
  // The physical plan is either retrieved from the SQLPhysicalPlanCache or, if unavailable, translated from the
  // optimized LQP.
  const std::shared_ptr<AbstractOperator>& get_physical_plan();

  // Returns all tasks that need to be executed for this query.
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

  const std::shared_ptr<LQPTranslator> _lqp_translator;
  const std::shared_ptr<Optimizer> _optimizer;

  // Execution results
  std::shared_ptr<hsql::SQLParserResult> _parsed_sql_statement;
  std::shared_ptr<AbstractLQPNode> _unoptimized_logical_plan;
  std::shared_ptr<AbstractLQPNode> _optimized_logical_plan;
  std::shared_ptr<AbstractOperator> _physical_plan;
  std::vector<std::shared_ptr<OperatorTask>> _tasks;
  std::shared_ptr<const Table> _result_table;
  // Assume there is an output table. Only change if nullptr is returned from execution.
  bool _query_has_output = true;

  std::shared_ptr<SQLPipelineStatementMetrics> _metrics;

  // Delete temporary tables
  const CleanupTemporaries _cleanup_temporaries;
};

}  // namespace opossum
