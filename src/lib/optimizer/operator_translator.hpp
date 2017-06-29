#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class OperatorTranslator {
 public:
  static OperatorTranslator &get();

  OperatorTranslator(OperatorTranslator const &) = delete;
  OperatorTranslator &operator=(const OperatorTranslator &) = delete;
  OperatorTranslator(OperatorTranslator &&) = delete;

  static const std::shared_ptr<AbstractOperator> translate_operator(std::shared_ptr<AbstractNode> node);

 protected:
  OperatorTranslator();
  OperatorTranslator &operator=(OperatorTranslator &&) = default;

 private:
  const std::shared_ptr<AbstractOperator> translate_table_node(std::shared_ptr<AbstractNode> node) const;
  const std::shared_ptr<AbstractOperator> translate_table_scan_node(std::shared_ptr<AbstractNode> node) const;
  const std::shared_ptr<AbstractOperator> translate_projection_node(std::shared_ptr<AbstractNode> node) const;

 private:
  static std::unordered_map<NodeType, std::function<std::shared_ptr<AbstractOperator>(std::shared_ptr<AbstractNode>)>>
      _operator_factory;
};

}  // namespace opossum
