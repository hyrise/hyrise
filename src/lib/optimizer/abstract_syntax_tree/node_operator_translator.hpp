#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class NodeOperatorTranslator {
 public:
  static NodeOperatorTranslator &get();

  NodeOperatorTranslator(NodeOperatorTranslator const &) = delete;
  NodeOperatorTranslator &operator=(const NodeOperatorTranslator &) = delete;
  NodeOperatorTranslator(NodeOperatorTranslator &&) = delete;

  const std::shared_ptr<AbstractOperator> translate_node(std::shared_ptr<AbstractAstNode> node) const;

 protected:
  NodeOperatorTranslator();
  NodeOperatorTranslator &operator=(NodeOperatorTranslator &&) = default;

 private:
  const std::shared_ptr<AbstractOperator> translate_table_node(std::shared_ptr<AbstractAstNode> node) const;
  const std::shared_ptr<AbstractOperator> translate_table_scan_node(std::shared_ptr<AbstractAstNode> node) const;
  const std::shared_ptr<AbstractOperator> translate_projection_node(std::shared_ptr<AbstractAstNode> node) const;
  const std::shared_ptr<AbstractOperator> translate_order_by_node(std::shared_ptr<AbstractAstNode> node) const;

 private:
  std::unordered_map<AstNodeType, std::function<std::shared_ptr<AbstractOperator>(std::shared_ptr<AbstractAstNode>)>>
      _operator_factory;
};

}  // namespace opossum
