#pragma once

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_parameter_variant.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {

/**
 * Produces an AST (Abstract Syntax Tree), as defined in optimizer/abstract_syntax_tree, from an hsql::SQLParseResult.
 *
 * The elements of the vector returned by SQLToASTTranslator::translate_parse_result(const hsql::SQLParserResult&)
 * point to the root/result nodes of the ASTs.
 *
 * An AST can either be handed to the optimizer, once it is added, or it can be directly turned into Operators by
 * the ASTToOperatorTranslator.
 *
 * Refer to sql_to_result_test.cpp for an example of the SQLToASTTranslator in proper action.
 * It is used as a Singleton via SQLToASTTranslator::get().
 *
 * The basic usage looks like this:
 *
 * hsql::SQLParserResult parse_result;
 * hsql::SQLParser::parseSQLString(params.query, &parse_result);
 * auto result_nodes = SQLToASTTranslator::get().translate_parse_result(parse_result);
 */
class SQLToASTTranslator final : public boost::noncopyable {
 public:
  static SQLToASTTranslator& get();

  // Translates the given SQL result.
  std::vector<std::shared_ptr<AbstractASTNode>> translate_parse_result(const hsql::SQLParserResult& result);

  std::shared_ptr<AbstractASTNode> translate_statement(const hsql::SQLStatement& statement);

  static AllParameterVariant translate_literal(const hsql::Expr& expr);
  static std::string generate_column_name(const hsql::Expr& expr, bool include_table_name);

 protected:
  std::shared_ptr<AbstractASTNode> _translate_select(const hsql::SelectStatement& select);

  std::shared_ptr<AbstractASTNode> _translate_table_ref(const hsql::TableRef& table);

  std::shared_ptr<AbstractASTNode> _translate_filter_expr(const hsql::Expr& expr,
                                                          const std::shared_ptr<AbstractASTNode>& input_node);

  std::shared_ptr<AbstractASTNode> _translate_aggregate(const hsql::SelectStatement& select,
                                                        const std::shared_ptr<AbstractASTNode>& input_node);

  std::shared_ptr<AbstractASTNode> _translate_projection(const std::vector<hsql::Expr*>& select_list,
                                                         const std::shared_ptr<AbstractASTNode>& input_node);

  std::shared_ptr<AbstractASTNode> _translate_order_by(const std::vector<hsql::OrderDescription*>& order_list,
                                                       const std::shared_ptr<AbstractASTNode>& input_node);

  std::shared_ptr<AbstractASTNode> _translate_join(const hsql::JoinDefinition& select);

  std::shared_ptr<AbstractASTNode> _translate_cross_product(const std::vector<hsql::TableRef*>& tables);

 private:
  SQLToASTTranslator() = default;
};

}  // namespace opossum
