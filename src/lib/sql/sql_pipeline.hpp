#pragma once

#include <string>

#include "SQLParserResult.h"
#include "concurrency/transaction_context.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * This is the unified interface to handle SQL queries and related operations.
 *
 * The basic idea of the SQLPipeline is that represents the flow from the basic SQL string to the result table with all
 * intermediate steps. These intermediate steps call the previous step that is required. The intermediate results are
 * all cached so calling a method twice will return the already existing value.
 *
 * The SQLPipeline holds all results and only hands them out as const references. If the SQLPipeline goes out of scope
 * while the results are still needed, the results are either nullptr or undefined.
 *
 * E.g: calling sql_pipeline.get_result_table() will result in the following "call stack"
 * get_result_table -> get_tasks -> get_query_plan -> get_optimized_logical_plan -> get_parsed_sql
 */
class SQLPipeline : public Noncopyable {
 public:
  // An explicitly passed TransactionContext will result in no auto-commit and LQP validation.
  SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context);

  // If use_mvcc is true, a Transaction context is created with auto-commit and LQP validation.
  // If false, no validation or transaction management is done.
  explicit SQLPipeline(const std::string& sql, bool use_mvcc = true);

  const std::string sql_string() { return _sql; }

  const hsql::SQLParserResult& get_parsed_sql();

  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_unoptimized_logical_plan();

  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_optimized_logical_plan();

  // For now, this always uses the optimized LQP.
  const SQLQueryPlan& get_query_plan();

  const std::vector<std::shared_ptr<OperatorTask>>& get_tasks();

  const std::shared_ptr<const Table>& get_result_table();

  const std::shared_ptr<TransactionContext>& transaction_context();

  double parse_time_seconds();
  double compile_time_seconds();
  double execution_time_seconds();

 private:
  const std::string _sql;

  // Execution results
  std::unique_ptr<hsql::SQLParserResult> _parsed_sql;
  std::vector<std::shared_ptr<AbstractLQPNode>> _unopt_logical_plan;
  std::vector<std::shared_ptr<AbstractLQPNode>> _opt_logical_plan;
  std::unique_ptr<SQLQueryPlan> _query_plan;
  std::vector<std::shared_ptr<OperatorTask>> _op_tasks;
  std::shared_ptr<const Table> _result_table;

  // Execution times
  double _parse_time_sec;
  double _compile_time_sec;
  double _execution_time_sec;

  // Transaction related
  const bool _use_mvcc;
  const bool _auto_commit;
  std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
