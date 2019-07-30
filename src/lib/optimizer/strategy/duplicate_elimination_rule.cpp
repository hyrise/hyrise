#include "duplicate_elimination_rule.hpp"

#include <map>
#include <string>

#include "boost/functional/hash.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

std::string DuplicateEliminationRule::name() const { return "Duplicate Elimination Rule"; }

void DuplicateEliminationRule::_print_traversal(const std::shared_ptr<AbstractLQPNode>& node) const {
  if(node){
    // if(true || node->type == LQPNodeType::StoredTable){
      std::cout<< node->description() << ", " << node << "\n";
    // }
    _print_traversal(node->left_input());
    _print_traversal(node->right_input());
  }
}

void DuplicateEliminationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  std::cout << "DuplicateEliminationRule\n";
  _original_replacement_pairs.clear();
  _remaining_stored_table_nodes.clear();
  _sub_plans.clear();
  const auto duplicate_afflicted_lqp = node->deep_copy();

  _find_sub_plan_duplicates_traversal(node);

  for (const auto& [original, replacement] : _original_replacement_pairs) {
    for (const auto& output : original->outputs()) {
      const auto& input_side = original->get_input_side(output);
      output->set_input(input_side, replacement);
      // std::cout << "replacement: " << original << " --> " << replacement << "\n";
    }
  }

  std::cout << "after modifications, old:\n";
  _print_traversal(duplicate_afflicted_lqp);
  std::cout << "after modifications, new:\n";
  _print_traversal(node);

  const auto node_mapping = lqp_create_node_mapping(duplicate_afflicted_lqp, node);

  std::cout << "_remaining_stored_table_nodes\n";
  for(const auto& st_node : _remaining_stored_table_nodes){
    std::cout << st_node->description() << ", " << st_node << "\n";
  }

  _adapt_expressions_traversal(node, node_mapping);



  // const auto mismatch = lqp_find_subplan_mismatch(duplicate_afflicted_lqp, node);
  // if(mismatch){
  //   std::cout << "mismatch found:\n";
  //   std::cout << mismatch->first->description() << "\n"; 
  //   std::cout << mismatch->second->pdescription() << "\n"; 
  // }

}

// Alias [ca_county, d_year, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q1_q2_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q1_q2_increase, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q2_q3_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q2_q3_increase]
// rhs:
// Alias [ca_county, d_year, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q1_q2_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q1_q2_increase, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q2_q3_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q2_q3_increase]


// Alias [ca_county, d_year, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q1_q2_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q1_q2_increase, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q2_q3_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q2_q3_increase]
// rhs:
// Alias [ca_county, d_year, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q1_q2_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q1_q2_increase, (SUM(ws_ext_sales_price) * 1) / SUM(ws_ext_sales_price) AS web_q2_q3_increase, (SUM(ss_ext_sales_price) * 1) / SUM(ss_ext_sales_price) AS store_q2_q3_increase]


void DuplicateEliminationRule::_find_sub_plan_duplicates_traversal(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto left_node = node->left_input();
  const auto right_node = node->right_input();

  if (left_node) {
    _find_sub_plan_duplicates_traversal(left_node);
  }
  if (right_node) {
    _find_sub_plan_duplicates_traversal(right_node);
  }
  const auto duplicate_iter = std::find_if(_sub_plans.cbegin(), _sub_plans.cend(),
                               [&node](const auto& sub_plan) { return *node == *sub_plan; });
  // std::cout << "processing " << node->description() << ", " << node << "\n";
  if (duplicate_iter == _sub_plans.end()) {
    _sub_plans.emplace_back(node);
    // std::cout << "...stored in sub plans\n";
    if(node->type == LQPNodeType::StoredTable){
      _remaining_stored_table_nodes.emplace_back(node);
    }
  } else {
    _original_replacement_pairs.emplace_back(node, *duplicate_iter);
    // std::cout << "... found duplicate: " << node->description() << ", " << *duplicate_iter << "\n";
  }
}

void DuplicateEliminationRule::_adapt_expressions_traversal(
  const std::shared_ptr<AbstractLQPNode>& node, const LQPNodeMapping& node_mapping) const {
  if(node){
    expressions_adapt_to_different_lqp(node->node_expressions, node_mapping, _remaining_stored_table_nodes);
    _adapt_expressions_traversal(node->left_input(), node_mapping);
    _adapt_expressions_traversal(node->right_input(),node_mapping);
  }
}

}  // namespace opossum
