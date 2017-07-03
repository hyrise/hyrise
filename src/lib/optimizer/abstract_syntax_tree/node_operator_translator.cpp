#include "node_operator_translator.hpp"

#include <memory>

#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"

namespace opossum {

// singleton
NodeOperatorTranslator &NodeOperatorTranslator::get() {
  static NodeOperatorTranslator instance;
  return instance;
}

NodeOperatorTranslator::NodeOperatorTranslator() {
  _operator_factory[NodeType::Table] =
      std::bind(&NodeOperatorTranslator::translate_table_node, this, std::placeholders::_1);
  _operator_factory[NodeType::TableScan] =
      std::bind(&NodeOperatorTranslator::translate_table_scan_node, this, std::placeholders::_1);
  _operator_factory[NodeType::Projection] =
      std::bind(&NodeOperatorTranslator::translate_projection_node, this, std::placeholders::_1);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_node(
    std::shared_ptr<AbstractNode> node) const {
  auto it = _operator_factory.find(node->type());

  if (it == _operator_factory.end()) {
    throw std::runtime_error("No factory for NodeType.");
  }

  return it->second(node);
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_node(
    std::shared_ptr<AbstractNode> node) const {
  auto table_node = std::dynamic_pointer_cast<TableNode>(node);
  return std::make_shared<GetTable>(table_node->table_name());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_table_scan_node(
    std::shared_ptr<AbstractNode> node) const {
  auto input_operator = translate_node(node->left());
  auto table_scan_node = std::dynamic_pointer_cast<TableScanNode>(node);

  // TODO(tim): change to ScanType
  // return std::make_shared<TableScan>(input_operator, table_scan_node->column_name(), table_scan_node->scan_type(),
  return std::make_shared<TableScan>(input_operator, table_scan_node->column_name(), "=", table_scan_node->value(),
                                     table_scan_node->value2());
}

const std::shared_ptr<AbstractOperator> NodeOperatorTranslator::translate_projection_node(
    std::shared_ptr<AbstractNode> node) const {
  auto input_operator = translate_node(node->left());
  return std::make_shared<Projection>(input_operator, node->output_columns());
}

}  // namespace opossum
