#include "dips_pruning_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/abstract_expression.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"
#include "hyrise.hpp"
#include <iostream>


namespace opossum {


  void DipsPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
      
      // 1. bau den JoinGraph (Baum traversieren)
      // (1.1) Verhindere Zyklen
      // 2. Waehle Wurzel
      // 3. Child-Parent Pointers setzen
      // 4. Baum von unten nach oben durchgehen (Paper)
      // 5. Baum von oben nach unten durchgehen (Paper)

    // _clear_join_graph();

    std::shared_ptr<DipsJoinGraph> join_graph = std::make_shared<DipsJoinGraph>();
    _build_join_graph(node, join_graph);
    
    //std::cout << *join_graph << '\n';
    //std::cout << "Is tree: "<< join_graph->is_tree() << '\n';

    if(join_graph->is_empty()){
      //std::cout << "==== JOIN GRAPH IS EMPTY ====" << std::endl;
      return;
    }

    if(join_graph->is_tree()){
      std::shared_ptr<DipsJoinGraphNode> root = join_graph->nodes[0]; // TODO: finding root implementation
      //std::cout << "==== ROOT ====" << std::endl;
      //std::cout << "    " << root << std::endl;
      join_graph->set_root(root);
      bottom_up_dip_traversal(root);
      top_down_dip_traversal(root);
    } else {
      // TODO: cycle implementation
    }

    //std::cout << "\nAFTER OPERATIONS\n" << '\n';
    //std::cout << *join_graph << '\n';


    /*
    _apply_to_inputs(node);

    if (node->type == LQPNodeType::Join) {
      const auto& join_node = static_cast<JoinNode&>(*node);
      const auto join_predicates = join_node.join_predicates();
          
      for (auto predicate : join_predicates) {

        std::shared_ptr<BinaryPredicateExpression> binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
        auto left_operand = binary_predicate->left_operand();
        auto right_operand = binary_predicate->right_operand();

        auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
        auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

        if (!left_lqp || !right_lqp) {
          return;
        }

        // TODO: check const pointer cast

        std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock()));
        std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock()));
        // int number_of_pruned_left = left_stored_table_node->pruned_chunk_ids().size();
        // int number_of_pruned_right = right_stored_table_node->pruned_chunk_ids().size();

        if (!left_stored_table_node || !right_stored_table_node) {
          return;
        }

        // LEFT -> RIGHT
        dips_pruning(left_stored_table_node, left_lqp->original_column_id, right_stored_table_node, right_lqp->original_column_id);

        // RIGHT -> LEFT
        dips_pruning(right_stored_table_node, right_lqp->original_column_id, left_stored_table_node, left_lqp->original_column_id);

        // std::cout << "Prune on " << left_stored_table_node->table_name << " Before: " << number_of_pruned_left << " After " << left_stored_table_node->pruned_chunk_ids().size() << std::endl;
        // std::cout << "Prune on " << right_stored_table_node->table_name << " Before: " << number_of_pruned_right << " After " << right_stored_table_node->pruned_chunk_ids().size() << std::endl;
      
      }
    }
    */
  }

  void DipsPruningRule::bottom_up_dip_traversal(std::shared_ptr<DipsJoinGraphNode> node) const { //expects root in the first call
    for (std::shared_ptr<DipsJoinGraphNode> child : node->children){
      bottom_up_dip_traversal(child);
    }
    if(node->parent == nullptr){  //handle root
      return;
    }
    auto edge = node->get_edge_for_table(node->parent);

    for (auto predicate : edge->predicates){
      auto left_operand = predicate->left_operand();
      auto right_operand = predicate->right_operand();

      auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
      auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

      std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock()));
      std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock()));
      
      if (!left_stored_table_node || !right_stored_table_node) {
          return;
      }

      // LEFT -> RIGHT
      dips_pruning(left_stored_table_node, left_lqp->original_column_id, right_stored_table_node, right_lqp->original_column_id);

      // RIGHT -> LEFT
      dips_pruning(right_stored_table_node, right_lqp->original_column_id, left_stored_table_node, left_lqp->original_column_id);
    } 
  }

  void DipsPruningRule::top_down_dip_traversal(std::shared_ptr<DipsJoinGraphNode> node) const { //expects root in the first call
    if(node->parent == nullptr){  //handle root
      return;
    }
    auto edge = node->get_edge_for_table(node->parent);

    for (auto predicate : edge->predicates){
      auto left_operand = predicate->left_operand();
      auto right_operand = predicate->right_operand();

      auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
      auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

      std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock()));
      std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock()));
      
      if (!left_stored_table_node || !right_stored_table_node) {
          return;
      }

      // LEFT -> RIGHT
      dips_pruning(left_stored_table_node, left_lqp->original_column_id, right_stored_table_node, right_lqp->original_column_id);

      // RIGHT -> LEFT
      dips_pruning(right_stored_table_node, right_lqp->original_column_id, left_stored_table_node, left_lqp->original_column_id);
    } 

    for (std::shared_ptr<DipsJoinGraphNode> child : node->children){
      bottom_up_dip_traversal(child);
    }
  }


  void DipsPruningRule::_build_join_graph(const std::shared_ptr<AbstractLQPNode>& node, std::shared_ptr<DipsJoinGraph> join_graph) const {
    if (node->left_input()) _build_join_graph(node->left_input(), join_graph);
    if (node->right_input()) _build_join_graph(node->right_input(), join_graph);

    if (node->type == LQPNodeType::Join) {
      const auto& join_node = static_cast<JoinNode&>(*node);
      const auto join_predicates = join_node.join_predicates();

      for (auto predicate : join_predicates) {

        std::shared_ptr<BinaryPredicateExpression> binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
        auto left_operand = binary_predicate->left_operand();
        auto right_operand = binary_predicate->right_operand();

        auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
        auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

        if (!left_lqp || !right_lqp) {
          continue;
        }
        std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock()));

        std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock()));

        // access join graph nodes
        auto left_join_graph_node = join_graph->get_node_for_table(left_stored_table_node);
        auto right_join_graph_node = join_graph->get_node_for_table(right_stored_table_node);
        
        // access edges
        auto left_right_edge = left_join_graph_node->get_edge_for_table(right_join_graph_node);
        auto right_left_edge = right_join_graph_node->get_edge_for_table(left_join_graph_node);

        // append predicates
        left_right_edge->append_predicate(binary_predicate); // TODO: visit every node in LQP only once (avoid cycles) -> use "simple" append
        right_left_edge->append_predicate(binary_predicate);

        // left_right_edge->predicates.push_back(predicate);
        // right_left_edge->predicates.push_back(predicate);
        
      }
    }
  }

  void DipsPruningRule::extend_pruned_chunks( std::shared_ptr<StoredTableNode> table_node, std::set<ChunkID> pruned_chunk_ids) const
  {
    const auto& already_pruned_chunk_ids = table_node->pruned_chunk_ids();
    
    if (!already_pruned_chunk_ids.empty()) {
      std::vector<ChunkID> union_values;
      std::set_union(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), pruned_chunk_ids.begin(),
                          pruned_chunk_ids.end(), std::back_inserter(union_values));
      table_node->set_pruned_chunk_ids(union_values);
    } else {
      table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
    }
  }


  void DipsPruningRule::dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id) const {
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

    resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      // TODO: check if pointers would be more efficient
      auto base_ranges = get_not_pruned_range_statistics<ColumnDataType>(table_node, column_id);
      auto partner_ranges = get_not_pruned_range_statistics<ColumnDataType>(join_partner_table_node, join_partner_column_id);
      auto pruned_chunks = calculate_pruned_chunks<ColumnDataType>(base_ranges, partner_ranges);
      extend_pruned_chunks(join_partner_table_node, pruned_chunks);
    });
  } 

  std::ostream& operator<<(std::ostream& stream, const DipsJoinGraph join_graph) {
      stream << "==== Vertices ====" << std::endl;
    if (join_graph.nodes.empty()) {
      stream << "<none>" << std::endl;
    } else {
      for (const auto node : join_graph.nodes) {
        stream << node->table_node->description() << std::endl;
        stream << "      ==== Adress ====" << std::endl;
        stream << "          " << node << std::endl;
        stream << "      ==== Parent ====" << std::endl;
        stream << "          " << node->parent << std::endl;
        stream << "      ==== Children ====" << std::endl;
        for (auto child : node->children){
          stream << "          " << child << std::endl;
        }

        stream << "      ==== Edges ====" << std::endl;
        for (const auto edge : node->edges) {
          stream << "      " << edge->partner_node->table_node->description() << std::endl;
          stream << "            ==== Predicates ====" << std::endl;
          for (auto predicate : edge->predicates) {
            stream << "            " << predicate->description(AbstractExpression::DescriptionMode::ColumnName) << std::endl;
          }
        }
      }
    }

    return stream;
  }



}