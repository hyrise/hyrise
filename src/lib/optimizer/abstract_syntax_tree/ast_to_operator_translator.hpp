#pragma once

#include <boost/noncopyable.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ASTToOperatorTranslator final : public boost::noncopyable {
 public:
  static ASTToOperatorTranslator &get();

  ASTToOperatorTranslator();

  const std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractASTNode> &node) const;

 private:
  const std::shared_ptr<AbstractOperator> translate_table_node(const std::shared_ptr<AbstractASTNode> &node) const;
  const std::shared_ptr<AbstractOperator> translate_table_scan_node(const std::shared_ptr<AbstractASTNode> &node) const;
  const std::shared_ptr<AbstractOperator> translate_projection_node(const std::shared_ptr<AbstractASTNode> &node) const;
  const std::shared_ptr<AbstractOperator> translate_order_by_node(const std::shared_ptr<AbstractASTNode> &node) const;
  const std::shared_ptr<AbstractOperator> translate_join_node(const std::shared_ptr<AbstractASTNode> &node) const;
  const std::shared_ptr<AbstractOperator> translate_aggregate_node(const std::shared_ptr<AbstractASTNode> &node) const;

 private:
  std::unordered_map<ASTNodeType, std::function<std::shared_ptr<AbstractOperator>(std::shared_ptr<AbstractASTNode>)>>
      _operator_factory;
};

}  // namespace opossum
