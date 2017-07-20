#pragma once

#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "all_parameter_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

// The SQLQueryTranslator translates SQLStatement objects into an Hyrise operator trees.
// The operator trees are added to a SQLQueryPlan by their roots.
// This object can not be used concurrently.
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

  // Translates the give SQL result.
  // Adds the generated operator trees to the SQLQueryPlan.
  // Returns false, if an error occurred.
  bool translate_parse_result(const hsql::SQLParserResult& result);

  // Translates the single given SQL statement.
  // Adds the generated operator tree to the SQLQueryPlan.
  // Returns false, if an error occurred.
  bool translate_statement(const hsql::SQLStatement& statement);

  static const AllParameterVariant translate_literal(const hsql::Expr& expr);

 protected:
  bool _translate_select(const hsql::SelectStatement& select);

  // Evaluates the expression and pushes one or more TableScans onto
  // the tasks list. AND expressions are chained TableScans.
  // OR expressions are not supported yet.
  bool _translate_filter_expr(const hsql::Expr& expr, const std::shared_ptr<AbstractOperator>& input_op);

  bool _translate_projection(const std::vector<hsql::Expr*>& expr_list,
                             const std::shared_ptr<AbstractOperator>& input_op);

  bool _translate_group_by(const hsql::GroupByDescription& group_by, const std::vector<hsql::Expr*>& select_list,
                           const std::shared_ptr<AbstractOperator>& input_op);

  bool _translate_order_by(const std::vector<hsql::OrderDescription*> order_list,
                           const std::shared_ptr<AbstractOperator>& input_op);

  bool _translate_table_ref(const hsql::TableRef& table);

  static bool _translate_filter_op(const hsql::Expr& expr, ScanType* output);

  static std::string _get_column_name(const hsql::Expr& expr);

  // Generated execution plan.
  SQLQueryPlan _plan;

  // Current root of the operator tree that is being built.
  std::shared_ptr<AbstractOperator> _current_root;

  // Details about the error, if one occurred.
  std::string _error_msg;
};

}  // namespace opossum
