#pragma once

#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "scheduler/operator_task.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

// The SQLQueryTranslator offers functionality to translate a
// SQLStatement or SQLParserResult object into a SQLQueryPlan.
// The translator should not be used concurrently.
class SQLQueryTranslator {
 public:
  SQLQueryTranslator();
  virtual ~SQLQueryTranslator();

  // Returns the list of tasks that were created during translation.
  const SQLQueryPlan& get_query_plan();

  // Get the error message, if any exists.
  const std::string& get_error_msg();

  // Destroy the currently stored execution plan and state.
  void reset();

  // Translates the give SQL result. Adds the generated execution plan to _tasks.
  bool translate_parse_result(const hsql::SQLParserResult& result);

  // Translates the single given SQL statement. Adds the generated execution plan to _tasks.
  bool translate_statement(const hsql::SQLStatement& statement);

 protected:
  bool _translate_select(const hsql::SelectStatement& select);

  // Evaluates the expression and pushes one or more TableScans onto
  // the tasks list. AND expressions are chained TableScans.
  // OR expressions are not supported yet.
  bool _translate_filter_expr(const hsql::Expr& expr, const std::shared_ptr<OperatorTask>& input_task);

  bool _translate_projection(const std::vector<hsql::Expr*>& expr_list,
                             const std::shared_ptr<OperatorTask>& input_task);

  bool _translate_order_by(const std::vector<hsql::OrderDescription*> order_list,
                           const std::shared_ptr<OperatorTask>& input_task);

  bool _translate_table_ref(const hsql::TableRef& table);

  static bool _translate_literal(const hsql::Expr& expr, AllTypeVariant* output);

  static bool _translate_filter_op(const hsql::Expr& expr, std::string* output);

  static std::string _get_column_name(const hsql::Expr& expr);

  // Generated execution plan.
  SQLQueryPlan _plan;

  // Details about the error, if one occurred.
  std::string _error_msg;
};

}  // namespace opossum
