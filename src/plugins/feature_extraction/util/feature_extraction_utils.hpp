#pragma once

#include <queue>
#include <unordered_set>

#include <magic_enum.hpp>

#include "expression/lqp_column_expression.hpp"
#include "feature_extraction/feature_nodes/abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/table_feature_node.hpp"
#include "feature_extraction/feature_types.hpp"

namespace opossum {

template <typename EnumType>
std::shared_ptr<FeatureVector> one_hot_encoding(const EnumType value) {
  auto result = std::make_shared<FeatureVector>(magic_enum::enum_count<EnumType>());
  const auto& index = magic_enum::enum_index(value);
  Assert(index, "No index found for '" + std::string{magic_enum::enum_name(value)} + "'");
  (*result)[*index] = Feature{1};
  return result;
}

template <typename EnumType>
std::vector<std::string> one_hot_headers(const std::string& prefix) {
  const auto& enum_names = magic_enum::enum_names<EnumType>();
  auto result = std::vector<std::string>{};
  result.reserve(enum_names.size());
  for (const auto& entry_name : enum_names) {
    result.emplace_back(prefix + std::string{entry_name});
  }
  return result;
}

enum class FeatureNodeVisitation { VisitInputs, DoNotVisitInputs };

/**
 * Calls the passed @param visitor on @param feature_npde and recursively on its INPUTS.
 * The visitor returns `FeatureNodeVisitation`, indicating whether the current node's input should be visited
 * as well. The algorithm is breadth-first search.
 * Each node is visited exactly once.
 *
 * @param Visitor      Functor called with every operator as a param.
 *                     Returns `FeatureNodeVisitation`
 */
template <typename FeaturNode, typename Visitor>
void visit_feature_nodes(const std::shared_ptr<FeaturNode>& feature_node, Visitor visitor) {
  using AbstractFeatureNodeType =
      std::conditional_t<std::is_const_v<FeaturNode>, const AbstractFeatureNode, AbstractFeatureNode>;

  std::queue<std::shared_ptr<AbstractFeatureNodeType>> node_queue;
  node_queue.push(feature_node);

  std::unordered_set<std::shared_ptr<AbstractFeatureNodeType>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == FeatureNodeVisitation::VisitInputs) {
      if (node->left_input()) {
        node_queue.push(node->left_input());
      }
      if (node->right_input()) {
        node_queue.push(node->right_input());
      }
    }
  }
}

std::vector<std::shared_ptr<TableFeatureNode>> find_base_tables(const std::shared_ptr<AbstractFeatureNode>& root_node);

std::shared_ptr<TableFeatureNode> match_base_table(const LQPColumnExpression& column_expression,
                                                   const std::vector<std::shared_ptr<TableFeatureNode>>& base_tables);

}  // namespace opossum
