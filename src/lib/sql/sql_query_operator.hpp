#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "sql/ExecuteStatement.h"
#include "sql/PrepareStatement.h"

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "sql/sql_result_operator.hpp"

namespace opossum {

// The SQLQueryOperator takes a SQL query, parses and transforms it.
//
// If schedule_plan is 'true', it will automatically schedule the resulting execution plan.
// To get the result of the execution plan, it exposes an SQLResultOperator task, which
// will upon completion contain the result table of the query.
//
// If schedule_plan is 'false', the plan will not automatically be scheduled. Instead it may
// be retrieved by calling get_query_plan(). This is useful if you would like to manually
// modify the query plan before execution or use a different execution/scheduling approach
// than the default. This is also used for testing and benchmarking purposes.
class SQLQueryOperator : public AbstractOperator {
 public:
  explicit SQLQueryOperator(const std::string& query, bool schedule_plan = true);

  const std::string name() const override;

  uint8_t num_in_tables() const override;

  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  const std::shared_ptr<OperatorTask>& get_result_task() const;

  bool parse_tree_cache_hit() const;

  bool query_plan_cache_hit() const;

  // Return the generated query plan.
  const SQLQueryPlan& get_query_plan() const;

  // Static. Return the running instance of the parse tree cache.
  static SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>>& get_parse_tree_cache();

  static SQLQueryCache<SQLQueryPlan>& get_query_plan_cache();

  static SQLQueryCache<SQLQueryPlan>& get_prepared_statement_cache();

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;

  std::shared_ptr<hsql::SQLParserResult> parse_query(const std::string& query);

  // Compiles the string into a SQLQueryPlan stored in _plan.
  void compile_query(const std::string& query);

  // Compiles the given parse result into an operator plan.
  void compile_parse_result(std::shared_ptr<hsql::SQLParserResult> result);

  // Translates the query that is supposed to be prepared and saves it
  // in the prepared statement cache by its name.
  void prepare_statement(const hsql::PrepareStatement& prepare_stmt);

  // Tries to fetch the referenced prepared statement and retrieve its cached data.
  void execute_prepared_statement(const hsql::ExecuteStatement& execute_stmt);

  // Translate the statement and append the result plan
  // to the current total query plan (in member _plan).
  void plan_statement(const hsql::SQLStatement& stmt);

  // Raw SQL query string.
  const std::string _query;

  // Result operator, which will be dependent on the full execution of the exec plan.
  std::shared_ptr<SQLResultOperator> _result_op;

  // Operator task, which wraps the result operator.
  std::shared_ptr<OperatorTask> _result_task;

  // Resulting query plan that will be populated during compilation.
  SQLQueryPlan _plan;

  // True, if the generated plan will automatically be scheduled by the operator.
  const bool _schedule_plan;

  // True, if the parse tree was obtained from the cache.
  bool _parse_tree_cache_hit;

  // True, if the query plan was obtained from the cache.
  bool _query_plan_cache_hit;

  // Static.
  // Automatic caching of parse trees during runtime.
  static SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>> _parse_tree_cache;

  // Static.
  // Stores all user defined prepared statements.
  static SQLQueryCache<SQLQueryPlan> _prepared_stmts;

  // Static.
  // Automatic caching of query plans during runtime.
  static SQLQueryCache<SQLQueryPlan> _query_plan_cache;
};

}  // namespace opossum
