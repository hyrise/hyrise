#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_parameter_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "parameter_id_allocator.hpp"
#include "sql_identifier_resolver.hpp"
#include "sql_identifier_resolver_proxy.hpp"
#include "storage/lqp_view.hpp"

namespace opossum {

class AggregateNode;
class LQPSubqueryExpression;

/**
 * Produces an LQP (Logical Query Plan), as defined in src/logical_query_plan/, from an hsql::SQLParseResult.
 *
 * The elements of the vector returned by SQLTranslator::translate_parse_result(const hsql::SQLParserResult&)
 * point to the root/result nodes of the LQPs.
 *
 * An LQP can either be handed to the Optimizer, or it can be directly turned into Operators by the LQPTranslator.
 */
class SQLTranslator final {
 public:
  /**
   * @param use_mvcc  Whether ValidateNodes should be compiled into the plan
   */
  explicit SQLTranslator(const UseMvcc use_mvcc);

  /**
   * @return after translate_*(), contains the ParameterIDs allocated for the placeholders in the query
   */
  std::vector<ParameterID> parameter_ids_of_value_placeholders() const;

  /**
   * Main entry point. Translate an AST produced by the SQLParser into LQPs, one for each SQL statement
   */
  std::vector<std::shared_ptr<AbstractLQPNode>> translate_parser_result(const hsql::SQLParserResult& result);

  /**
   * Translate an Expression AST into a Hyrise-expression. No columns can be referenced in expressions translated by
   * this call.
   */
  static std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& hsql_expr, const UseMvcc use_mvcc);

 private:
  // An expression and its identifiers. This is partly redundant to the SQLIdentifierResolver, but allows expressions
  // for equal SQL expressions with different identifiers (e.g., SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2 FROM ...).
  struct SelectListElement {
    explicit SelectListElement(const std::shared_ptr<AbstractExpression>& expression);
    SelectListElement(const std::shared_ptr<AbstractExpression>& expression,
                      const std::vector<SQLIdentifier>& identifiers);

    std::shared_ptr<AbstractExpression> expression;
    std::vector<SQLIdentifier> identifiers;
  };

  // Track state while translating the FROM clause. This makes sure only the actually available SQL identifiers can be
  // used, e.g. "SELECT * FROM t1, t2 JOIN t3 ON t1.a = t2.a" is illegal since t1 is invisible to the seconds entry.
  // Also ensures the correct columns go into Select wildcards, even in presence of NATURAL/SEMI joins that remove
  // columns from input tables
  struct TableSourceState final {
    TableSourceState() = default;
    TableSourceState(const std::shared_ptr<AbstractLQPNode>& lqp,
                     const std::unordered_map<std::string, std::vector<SelectListElement>>& elements_by_table_name,
                     const std::vector<SelectListElement>& elements_in_order,
                     const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver);

    void append(TableSourceState&& rhs);

    std::shared_ptr<AbstractLQPNode> lqp;

    // Collects the output of the FROM clause to expand wildcards (*; <t>.*) used in the SELECT list
    std::unordered_map<std::string, std::vector<SelectListElement>> elements_by_table_name;

    // To establish the correct order of columns in SELECT *
    std::vector<SelectListElement> elements_in_order;

    std::shared_ptr<SQLIdentifierResolver> sql_identifier_resolver;
  };

  // Represents the '*'/'<table>.*' wildcard in a Query. The SQLParser regards it as an Expression, but to Hyrise it
  // isn't one
  struct SQLWildcard final {
    std::optional<std::string> table_name;
  };

  /**
   * Internal constructor to create an SQLTranslator used for Subqueries
   * @param use_mvcc                                Whether ValidateNodes should be compiled into the plan
   * @param external_sql_identifier_resolver_proxy  Set during recursive invocations to resolve external identifiers
   *                                                in correlated subqueries
   * @param parameter_id_counter                    Set during recursive invocations to allocate unique ParameterIDs
   *                                                for each encountered parameter
   * @param with_descriptions                       Contains a mapping of LQPs and associated WITH aliases, which
   *                                                already got evaluated
   */
  SQLTranslator(const UseMvcc use_mvcc,
                const std::shared_ptr<SQLIdentifierResolverProxy>& external_sql_identifier_resolver_proxy,
                const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                const std::unordered_map<std::string, std::shared_ptr<LQPView>>& with_descriptions);

  std::shared_ptr<AbstractLQPNode> _translate_statement(const hsql::SQLStatement& statement);
  std::shared_ptr<AbstractLQPNode> _translate_select_statement(const hsql::SelectStatement& select);

  void _translate_hsql_with_description(hsql::WithDescription& desc);
  TableSourceState _translate_table_ref(const hsql::TableRef& hsql_table_ref);
  TableSourceState _translate_table_origin(const hsql::TableRef& hsql_table_ref);
  std::shared_ptr<AbstractLQPNode> _translate_stored_table(
      const std::string& name, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver);
  TableSourceState _translate_predicated_join(const hsql::JoinDefinition& join);
  TableSourceState _translate_natural_join(const hsql::JoinDefinition& join);
  TableSourceState _translate_cross_product(const std::vector<hsql::TableRef*>& tables);

  std::vector<SelectListElement> _translate_select_list(const std::vector<hsql::Expr*>& select_list);
  void _translate_select_groupby_having(const hsql::SelectStatement& select,
                                        const std::vector<SelectListElement>& select_list_elements);

  void _translate_order_by(const std::vector<hsql::OrderDescription*>& order_list);
  void _translate_limit(const hsql::LimitDescription& limit);

  std::shared_ptr<AbstractLQPNode> _translate_insert(const hsql::InsertStatement& insert);
  std::shared_ptr<AbstractLQPNode> _translate_delete(const hsql::DeleteStatement& delete_statement);
  std::shared_ptr<AbstractLQPNode> _translate_update(const hsql::UpdateStatement& update);

  std::shared_ptr<AbstractLQPNode> _translate_create(const hsql::CreateStatement& create_statement);
  std::shared_ptr<AbstractLQPNode> _translate_create_view(const hsql::CreateStatement& create_statement);
  std::shared_ptr<AbstractLQPNode> _translate_create_table(const hsql::CreateStatement& create_statement);

  std::shared_ptr<AbstractLQPNode> _translate_drop(const hsql::DropStatement& drop_statement);

  std::shared_ptr<AbstractLQPNode> _translate_prepare(const hsql::PrepareStatement& prepare_statement);
  std::shared_ptr<AbstractLQPNode> _translate_execute(const hsql::ExecuteStatement& execute_statement);

  std::shared_ptr<AbstractLQPNode> _translate_import(const hsql::ImportStatement& import_statement);

  std::shared_ptr<AbstractLQPNode> _translate_predicate_expression(
      const std::shared_ptr<AbstractExpression>& expression, std::shared_ptr<AbstractLQPNode> current_node) const;

  std::shared_ptr<AbstractLQPNode> _translate_show(const hsql::ShowStatement& show_statement);

  std::shared_ptr<AbstractLQPNode> _validate_if_active(const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractExpression> _translate_hsql_expr(
      const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const;
  std::shared_ptr<LQPSubqueryExpression> _translate_hsql_subquery(
      const hsql::SelectStatement& select, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const;
  std::shared_ptr<AbstractExpression> _translate_hsql_case(
      const hsql::Expr& expr, const std::shared_ptr<SQLIdentifierResolver>& sql_identifier_resolver) const;

  std::shared_ptr<AbstractExpression> _inverse_predicate(const AbstractExpression& expression) const;

  static std::shared_ptr<AbstractLQPNode> _prune_expressions(
      const std::shared_ptr<AbstractLQPNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  static std::shared_ptr<AbstractLQPNode> _add_expressions_if_unavailable(
      const std::shared_ptr<AbstractLQPNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  static std::vector<std::shared_ptr<AbstractExpression>> _unwrap_elements(
      const std::vector<SelectListElement>& select_list_elements);

 private:
  const UseMvcc _use_mvcc;

  std::shared_ptr<AbstractLQPNode> _current_lqp;
  std::shared_ptr<SQLIdentifierResolver> _sql_identifier_resolver;
  std::shared_ptr<SQLIdentifierResolverProxy> _external_sql_identifier_resolver_proxy;
  std::shared_ptr<ParameterIDAllocator> _parameter_id_allocator;
  std::optional<TableSourceState> _from_clause_result;
  std::unordered_map<std::string, std::shared_ptr<LQPView>> _with_descriptions;

  // "Inflated" because all wildcards will be inflated to the expressions they actually represent
  std::vector<SelectListElement> _inflated_select_list_elements;
};

}  // namespace opossum
