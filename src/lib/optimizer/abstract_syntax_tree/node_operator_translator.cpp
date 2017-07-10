#include "node_operator_translator.hpp"

#include <memory>

#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

namespace opossum {

// singleton
NodeOperatorTranslator &NodeOperatorTranslator::get() {
  static NodeOperatorTranslator instance;
  return instance;
}

NodeOperatorTranslator::NodeOperatorTranslator() {
  _operator_factory[AstNodeType::Table] =
      std::bind(&NodeOperatorTranslator::translate_table_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Predicate] =
      std::bind(&NodeOperatorTranslator::translate_table_scan_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Projection] =
      std::bind(&NodeOperatorTranslator::translate_projection_node, this, std::placeholders::_1);
  _operator_factory[AstNodeType::Sort] =
      std::bind(&NodeOperatorTranslator::translate_order_by_node, this, std::placeholders::_1);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto it = _operator_factory.find(node->type());

  if (it == _operator_factory.end()) {
    throw std::runtime_error("No factory for AstNodeType.");
  }

  return it->second(node);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto table_node = std::dynamic_pointer_cast<TableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_scan_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  return std::make_shared<TableScan>(input_operator, predicate_node->column_name(), predicate_node->scan_type(),
                                     predicate_node->value(), predicate_node->value2());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_projection_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());
  return std::make_shared<Projection>(input_operator, node->output_columns());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_order_by_node(
    std::shared_ptr<AbstractAstNode> node) const {
  auto input_operator = translate_node(node->left());

  auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  return std::make_shared<Sort>(input_operator, sort_node->column_name(), sort_node->asc());
}

}  // namespace opossum
