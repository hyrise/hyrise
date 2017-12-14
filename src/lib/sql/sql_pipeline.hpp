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
 * The basic idea of the SQLPipeline is that it represents the flow from the basic SQL string to the result table with
 * all intermediate steps. These intermediate steps call the previous step that is required. The intermediate results
 * are all cached so calling a method twice will return the already existing value.
 *
 * The SQLPipeline holds all results and only hands them out as const references. If the SQLPipeline goes out of scope
 * while the results are still needed, the result references are invalid (except maybe the result_table).
 *
 * E.g: calling sql_pipeline.get_result_table() will result in the following "call stack"
 * get_result_table -> get_tasks -> get_query_plan -> get_optimized_logical_plan -> get_parsed_sql
 */
class SQLPipeline : public Noncopyable {
 public:
  // If use_mvcc is true, a TransactionContext is created with auto-commit and validation during execution.
  // If false, no validation or transaction management is done.
  static std::vector<SQLPipeline> from_sql_string(const std::string& sql, bool use_mvcc = true);

  // An explicitly passed TransactionContext will result in no auto-commit and LQP validation.
  static std::vector<SQLPipeline> from_sql_string(const std::string& sql,
                                                  std::shared_ptr<TransactionContext> transaction_context);

  // Returns the parsed SQL string.
  const hsql::SQLParserResult& get_parsed_sql();

  // Returns all unoptimized LQP roots.
  const std::shared_ptr<AbstractLQPNode>& get_unoptimized_logical_plan();

  // Returns all optimized LQP roots.
  const std::shared_ptr<AbstractLQPNode>& get_optimized_logical_plan();

  // For now, this always uses the optimized LQP.
  const SQLQueryPlan& get_query_plan();

  // Returns all task sets that need to be executed for this query.
  const std::vector<std::shared_ptr<OperatorTask>>& get_tasks();

  // Executes all tasks, waits for them to finish, and returns the resulting table.
  const std::shared_ptr<const Table>& get_result_table();

  // Returns the TransactionContext that was either passed to or created by the SQLPipeline.
  // This can be a nullptr if no transaction management is wanted.
  const std::shared_ptr<TransactionContext>& transaction_context() const;

  std::chrono::microseconds compile_time_microseconds() const;
  std::chrono::microseconds execution_time_microseconds() const;

 private:
  static std::vector<SQLPipeline> _from_sql_string(const std::string& sql,
                                                   std::shared_ptr<TransactionContext> transaction_context,
                                                   bool use_mvcc);

  // Private constructor used by the static _from_sql_string method to create an SQLPipeline with exactly one statement
  SQLPipeline(std::unique_ptr<const hsql::SQLParserResult> parsed_sql,
              std::shared_ptr<TransactionContext> transaction_context, bool use_mvcc);

  static std::string _create_parse_error_message(const std::string& sql, const hsql::SQLParserResult& result);

  // Execution results
  std::unique_ptr<const hsql::SQLParserResult> _parsed_sql;
  std::shared_ptr<AbstractLQPNode> _unoptimized_logical_plan;
  std::shared_ptr<AbstractLQPNode> _optimized_logical_plan;
  std::unique_ptr<SQLQueryPlan> _query_plan;
  std::vector<std::shared_ptr<OperatorTask>> _tasks;
  std::shared_ptr<const Table> _result_table;
  // Assume there is an output table. Only change if nullptr is returned from execution.
  bool _query_has_output = true;

  // Execution times
  std::chrono::microseconds _compile_time_micro_sec;
  std::chrono::microseconds _execution_time_micro_sec;

  // Transaction related
  const bool _use_mvcc;
  const bool _auto_commit;
  std::shared_ptr<TransactionContext> _transaction_context;
};

}  // namespace opossum
