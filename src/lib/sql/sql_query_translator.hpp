#pragma once

#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "scheduler/operator_task.hpp"

namespace opossum {

// The SQLQueryTranslator offers functionality to parse a query string and
// transform it into an execution plan. This object should not be called
// concurrently.
class SQLQueryTranslator {
 public:
  SQLQueryTranslator();
  virtual ~SQLQueryTranslator();

  // Returns the list of tasks that were created during translation.
  const std::vector<std::shared_ptr<OperatorTask>>& get_tasks();

  // Get the error message, if any exists.
  const std::string& get_error_msg();

  // Destroy the currently stored execution plan and state.
  void reset();

  bool parse_query(const std::string& query, hsql::SQLParserResult* result);

  bool translate_query(const std::string& query);

  bool translate_parse_result(const hsql::SQLParserResult& result);

  bool translate_statement(const hsql::SQLStatement& statement);

 private:
  bool translate_select(const hsql::SelectStatement& select);

  // Evaluates the expression and pushes one or more TableScans onto
  // the tasks list. AND expressions are chained TableScans.
  // OR expressions are not supported yet.
  bool translate_filter_expr(const hsql::Expr& expr, const std::shared_ptr<OperatorTask>& input_task);

  bool translate_projection(const std::vector<hsql::Expr*>& expr_list, const std::shared_ptr<OperatorTask>& input_task);

  bool translate_literal(const hsql::Expr& expr, AllTypeVariant* output);

  bool translate_order_by(const std::vector<hsql::OrderDescription*> order_list,
                          const std::shared_ptr<OperatorTask>& input_task);

  bool translate_table_ref(const hsql::TableRef& table);

  bool translate_filter_op(const hsql::Expr& expr, std::string* output);

  std::vector<std::shared_ptr<OperatorTask>> _tasks;

  std::string _error_msg;
};

}  // namespace opossum
