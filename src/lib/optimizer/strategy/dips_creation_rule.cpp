#include "dips_creation_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include <iostream>

namespace opossum {

  void DipsCreationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
    if (node->type != LQPNodeType::Join) {
      _apply_to_inputs(node);
      return;
    }

    const auto& join_node = static_cast<JoinNode&>(*node);
    const auto join_predicates = join_node.join_predicates();

    left_column
    right_column

    iterate left_nodes
    find stored table with left column
    check_for prunes

    if pruned:
      check_statistics
      create_dips
      save_dips_to_opposite_side
    


    
    std::cout << "Predicates  for node: " << join_node << std::endl;
    for (auto predicate : join_predicates) {
      std::cout << "  " << predicate->description() << std::endl;
    }

    _apply_to_inputs(node);
  }
}