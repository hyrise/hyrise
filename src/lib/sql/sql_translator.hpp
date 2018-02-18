#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_parameter_variant.hpp"

#include "abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class AggregateNode;
class LQPExpression;

/**
* Produces an LQP (Logical Query Plan), as defined in src/logical_query_plan/, from an hsql::SQLParseResult.
 *
 * The elements of the vector returned by SQLTranslator::translate_parse_result(const hsql::SQLParserResult&)
 * point to the root/result nodes of the LQPs.
 *
 * An LQP can either be handed to the Optimizer, or it can be directly turned into Operators by the LQPTranslator.
 *
 *
 * ## ColumnID Resolution
 *
 * This translator resolves column names as used by SQL to ColumnIDs. For high level information check the blog post:
 * https://medium.com/hyrise/the-gentle-art-of-referring-to-columns-634f057bd810
 *
 * Most of the lifting for this is done in the overrides of
 * AbstractLQPNode::{get, find}_column_id_by_qualified_column_name() which Nodes that add, remove or rearrange columns
 * have to have an implementation of (Projection, Join, ...).
 * The handling of ColumnIdentifierName::table_name is also done in these overrides. StoredTableNode handles table
 * ALIASes and names (`SELECT t1.a, alias_t2.b FROM t1, t2 AS alias_t2`), ProjectionNode ALIASes for Expressions
 * (`SELECT a+b AS s [...]`) and AggregateNode ALIASes for AggregateFunctions(`SELECT SUM(a) AS s [...]`)
 *
 * To resolve Table wildcards such as `SELECT t1.* FROM t1, [...]` AbstractLQPNode::get_output_column_ids_for_table()
 * is used.
 *
 *
 * ## Usage
 * Refer to the SQLPlanner for an example of the SQLToASTTranslator in proper action.
 *
 * The basic usage looks like this:
 *
 * hsql::SQLParserResult parse_result;
 * hsql::SQLParser::parseSQLString(params.query, &parse_result);
 * auto result_nodes = SQLTranslator().translate_parse_result(parse_result);
 */
class SQLTranslator final : public Noncopyable {
 public:
  /**
   * @param validate If set to false, does not add validate nodes to the resulting tree.
   */
  explicit constexpr SQLTranslator(bool validate = true) : _validate{validate} {}

  // Translates the given SQL result.
  std::vector<std::shared_ptr<AbstractLQPNode>> translate_parse_result(const hsql::SQLParserResult& result);

  std::shared_ptr<AbstractLQPNode> translate_statement(const hsql::SQLStatement& statement);

 protected:
  std::shared_ptr<AbstractLQPNode> _translate_select(const hsql::SelectStatement& select);

  std::shared_ptr<AbstractLQPNode> _translate_table_ref(const hsql::TableRef& table);

  std::shared_ptr<AbstractLQPNode> _translate_where(const hsql::Expr& expr,
                                                    const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_having(const hsql::Expr& expr,
                                                     const std::shared_ptr<AggregateNode>& aggregate_node,
                                                     const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_aggregate(const hsql::SelectStatement& select,
                                                        const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_projection(const std::vector<hsql::Expr*>& select_list,
                                                         const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_order_by(const std::vector<hsql::OrderDescription*>& order_list,
                                                       const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_join(const hsql::JoinDefinition& select);
  std::shared_ptr<AbstractLQPNode> _translate_natural_join(const hsql::JoinDefinition& select);

  std::shared_ptr<AbstractLQPNode> _translate_cross_product(const std::vector<hsql::TableRef*>& tables);

  std::shared_ptr<AbstractLQPNode> _translate_limit(const hsql::LimitDescription& limit,
                                                    const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_insert(const hsql::InsertStatement& insert);

  std::shared_ptr<AbstractLQPNode> _translate_delete(const hsql::DeleteStatement& del);

  std::shared_ptr<AbstractLQPNode> _translate_update(const hsql::UpdateStatement& update);

  std::shared_ptr<AbstractLQPNode> _translate_create(const hsql::CreateStatement& update);

  std::shared_ptr<AbstractLQPNode> _translate_drop(const hsql::DropStatement& update);

  std::shared_ptr<AbstractLQPNode> _translate_table_ref_alias(const std::shared_ptr<AbstractLQPNode>& node,
                                                              const hsql::TableRef& table);

  /**
   * Helper function to avoid code duplication for WHERE and HAVING
   */
  std::shared_ptr<AbstractLQPNode> _translate_predicate(
      const hsql::Expr& hsql_expr, bool allow_function_columns,
      const std::function<LQPColumnReference(const hsql::Expr&)>& resolve_column,
      const std::shared_ptr<AbstractLQPNode>& input_node);

  std::shared_ptr<AbstractLQPNode> _translate_show(const hsql::ShowStatement& show_statement);

  std::shared_ptr<AbstractLQPNode> _validate_if_active(const std::shared_ptr<AbstractLQPNode>& input_node);

  std::vector<std::shared_ptr<LQPExpression>> _retrieve_having_aggregates(
      const hsql::Expr& expr, const std::shared_ptr<AbstractLQPNode>& input_node);

 private:
  const bool _validate;
};

}  // namespace opossum
