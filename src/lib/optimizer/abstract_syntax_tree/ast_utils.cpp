#include "ast_utils.hpp"

#include <map>
#include <memory>
#include <utility>

#include "boost/variant.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/type_utils.hpp"

namespace opossum {

bool ast_contains_join_edge(const std::shared_ptr<const AbstractASTNode>& node,
                            const std::shared_ptr<const AbstractASTNode>& descendant_a,
                            const std::shared_ptr<const AbstractASTNode>& descendant_b, ColumnID column_id_a,
                            ColumnID column_id_b, ScanType scan_type) {
  /**
   * Translate the local ColumnIDs column_id_a, column_id_b into "global" ColumnIDs in the output of node
   */
  const auto first_column_id_a = ast_get_first_column_id_of_descendant(node, descendant_a);
  const auto first_column_id_b = ast_get_first_column_id_of_descendant(node, descendant_b);

  return ast_contains_join_edge(node, make_column_id(first_column_id_a + column_id_a),
                                make_column_id(first_column_id_b + column_id_b), scan_type);
}

bool ast_contains_join_edge(const std::shared_ptr<const AbstractASTNode>& node, ColumnID column_id_a,
                            ColumnID column_id_b, ScanType scan_type) {
  // For now only consider Predicates and Joins when traversing the AST
  if (node->type() != ASTNodeType::Predicate && node->type() != ASTNodeType::Join) {
    return false;
  }

  if (node->type() == ASTNodeType::Predicate) {
    auto predicate_node = std::dynamic_pointer_cast<const PredicateNode>(node);

    if (predicate_node->value().type() == typeid(ColumnID)) {
      // Check whether the Predicate is the expression `column_id_a scan_type column_id_b`
      const auto in_column_order = predicate_node->column_id() == column_id_a &&
                                   boost::get<ColumnID>(predicate_node->value()) == column_id_b &&
                                   predicate_node->scan_type() == scan_type;

      // Check whether the Predicate is the expression `column_id_b flipped(scan_type) column_id_a`
      const auto in_flipped_column_order = predicate_node->column_id() == column_id_b &&
                                           boost::get<ColumnID>(predicate_node->value()) == column_id_a &&
                                           predicate_node->scan_type() == flip_scan_type(scan_type);

      if (in_column_order || in_flipped_column_order) {
        return true;
      }
    }

    return ast_contains_join_edge(node->left_child(), column_id_a, column_id_b, scan_type);
  }

  if (node->type() == ASTNodeType::Join) {
    auto join_node = std::dynamic_pointer_cast<const JoinNode>(node);

    const auto left_column_count = join_node->left_child()->output_column_count();

    if (join_node->join_mode() == JoinMode::Inner) {
      /**
       * If column_id_b falls into the right child, check whether the Join models `column_id_a scan_type column_id_b`
       * The check whether column_id_a falls into the left child happens implicitly when comparing the column_ids
       */
      if (static_cast<size_t>(column_id_b) >= left_column_count) {
        const auto in_column_order = join_node->join_column_ids() ==
                                         std::make_pair(column_id_a, make_column_id(column_id_b - left_column_count)) &&
                                     join_node->scan_type() == scan_type;
        if (in_column_order) {
          return true;
        }
      }

      /**
       * If column_id_a falls into the right child, check whether the Join models
       * `column_id_b flipped(scan_type) column_id_a`
       * The check whether column_id_b falls into the left child happens implicitly when comparing the column_ids
       */
      if (static_cast<size_t>(column_id_a) >= left_column_count) {
        const auto in_flipped_column_order =
            join_node->join_column_ids() ==
                std::make_pair(column_id_b, make_column_id(column_id_a - left_column_count)) &&
            join_node->scan_type() == flip_scan_type(scan_type);
        if (in_flipped_column_order) {
          return true;
        }
      }
    }

    // If both ColumnIDs fall into the left child, continue searching there
    if (static_cast<size_t>(column_id_a) < left_column_count && static_cast<size_t>(column_id_b) < left_column_count) {
      return ast_contains_join_edge(join_node->left_child(), column_id_a, column_id_b, scan_type);
    }

    // If both ColumnIDs fall into the right child, continue searching there
    if (static_cast<size_t>(column_id_a) >= left_column_count &&
        static_cast<size_t>(column_id_b) >= left_column_count) {
      return ast_contains_join_edge(join_node->right_child(), make_column_id(column_id_a - left_column_count),
                                    make_column_id(column_id_b - left_column_count), scan_type);
    }
  }

  return false;
}

ColumnID ast_get_first_column_id_of_descendant(const std::shared_ptr<const AbstractASTNode>& node,
                                               const std::shared_ptr<const AbstractASTNode>& descendant) {
  if (node == descendant) {
    return ColumnID{0};
  }

  if (node->type() != ASTNodeType::Predicate && node->type() != ASTNodeType::Join) {
    return INVALID_COLUMN_ID;
  }

  /**
   * Search in children
   */
  if (node->left_child()) {
    const auto column_id = ast_get_first_column_id_of_descendant(node->left_child(), descendant);
    if (column_id != INVALID_COLUMN_ID) {  // Only return if found, otherwise it might still be in the right node
      return column_id;
    }
  }

  if (node->right_child()) {
    const auto column_id = ast_get_first_column_id_of_descendant(node->right_child(), descendant);
    if (column_id != INVALID_COLUMN_ID) {
      return make_column_id(column_id + node->left_child()->output_column_count());
    }
  }

  return INVALID_COLUMN_ID;
}

ColumnIDMapping ast_generate_column_id_mapping(const ColumnOrigins& column_origins_a,
                                               const ColumnOrigins& column_origins_b) {
  DebugAssert(column_origins_a.size() == column_origins_b.size(), "Params must be shuffled set of each other");

  ColumnIDMapping output_mapping(column_origins_a.size(), INVALID_COLUMN_ID);
  std::map<ColumnOrigin, size_t> column_origin_to_input_idx;

  for (size_t column_idx = 0; column_idx < column_origins_a.size(); ++column_idx) {
    const auto result = column_origin_to_input_idx.emplace(column_origins_a[column_idx], column_idx);
    Assert(result.second, "The same column origin appears multiple times, can't create unambiguous mapping");
  }

  for (auto column_idx = ColumnID{0}; column_idx < column_origins_b.size(); ++column_idx) {
    auto iter = column_origin_to_input_idx.find(column_origins_b[column_idx]);
    DebugAssert(iter != column_origin_to_input_idx.end(), "This shouldn't happen.");
    output_mapping[iter->second] = column_idx;
  }

  return output_mapping;
}

}  // namespace opossum
