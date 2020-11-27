#include "dips_creation_rule.hpp"
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

  void DipsCreationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
    // if (node->type != LQPNodeType::Join) {
    //   _apply_to_inputs(node);
    //   return;
    // }
    _apply_to_inputs(node);

    if (node->type == LQPNodeType::Join) {
      const auto& join_node = static_cast<JoinNode&>(*node);
      const auto join_predicates = join_node.join_predicates();
          
      // std::unique_ptr<ChunkPruningRule> chunk_pruning = std::make_unique<ChunkPruningRule>();

      std::cout << "Predicates  for node: " << join_node << std::endl;
      for (auto predicate : join_predicates) {
        std::cout << "  " << predicate->description() << std::endl;

        std::shared_ptr<BinaryPredicateExpression> binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
        auto left_operand = binary_predicate->left_operand();
        auto right_operand = binary_predicate->right_operand();
        std::cout << "    " << left_operand->description() << std::endl;
        std::cout << "    " << right_operand->description() << std::endl;

        //TODO: make sure JOIN node has parents

        auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
        auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

        const std::shared_ptr<const StoredTableNode> left_stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock());
        const std::shared_ptr<const StoredTableNode> right_stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock());

        // LEFT -> RIGHT
        
        auto left_predicate_node = get_pruned_attribute_statistics(left_stored_table_node, left_lqp->original_column_id, right_lqp);
        auto left_parent_node = (right_stored_table_node->outputs())[0];

        if (left_parent_node->left_input() == right_stored_table_node) {
          lqp_insert_node(left_parent_node, LQPInputSide::Left, left_predicate_node);
        } 
        else {
          lqp_insert_node(left_parent_node, LQPInputSide::Right, left_predicate_node);
        }

        // chunk_pruning->apply_to(left_predicate_node);

        // RIGHT -> LEFT
        auto right_predicate_node = get_pruned_attribute_statistics(right_stored_table_node, right_lqp->original_column_id, left_lqp);
        auto right_parent_node = (left_stored_table_node->outputs())[0];



        if (right_parent_node->left_input() == left_stored_table_node) {
            lqp_insert_node(right_parent_node, LQPInputSide::Left, right_predicate_node);
        } 
        else {
          lqp_insert_node(right_parent_node, LQPInputSide::Right, right_predicate_node);
        }


      
        // chunk_pruning->apply_to(right_predicate_node);
      }

      // auto left_operand = (std::shared_ptr<BinaryPredicateExpression>) predicate->left_operand()
    }
  }

  std::shared_ptr<PredicateNode> DipsCreationRule::get_pruned_attribute_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<LQPColumnExpression> join_partner) const {
    auto pruned_chunks_ids = table_node->pruned_chunk_ids(); //const std::vector<ChunkID>&
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);
    std::vector<std::shared_ptr<AbstractExpression>> predicate_expressions;


    for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index){
       
      if(std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end())
      {
        auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
        resolve_data_type(chunk_statistic->data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;

          const auto segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(chunk_statistic);

          // Range filters are only available for arithmetic (non-string) types.
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            if (segment_statistics->range_filter) {
              for (const auto range: segment_statistics->range_filter->ranges)  {
                auto new_expression = expression_functional::between_inclusive_(join_partner, range.first, range.second);
                // auto new_predicate = PredicateNode::make(expression_functional::between_inclusive_(join_partner, range.first, range.second));
                predicate_expressions.push_back(new_expression);
              }
            }
            // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
            DebugAssert(!segment_statistics->min_max_filter,
                        "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
          }

        if (segment_statistics->min_max_filter) {
          auto new_expression = expression_functional::between_inclusive_(join_partner, segment_statistics->min_max_filter->min, segment_statistics->min_max_filter->max);
                // auto new_predicate = PredicateNode::make(expression_functional::between_inclusive_(join_partner, range.first, range.second));
          predicate_expressions.push_back(new_expression);
        }
        });
      } 
    }
    // TODO:
    // iterate over unpruned chunks
    // take chunk statistics
    // adjust predicate

    if (predicate_expressions.size() == 0) {
      return PredicateNode::make(expression_functional::or_(true, true));
    }
    auto collected_expression = build_logical_expression(predicate_expressions, 0);
    return PredicateNode::make(collected_expression);
  }

  std::shared_ptr<AbstractExpression> DipsCreationRule::build_logical_expression(std::vector<std::shared_ptr<AbstractExpression>> expressions, long unsigned int index) const {
    if (index < expressions.size() - 1){
      return expression_functional::or_(expressions[index], build_logical_expression(expressions, index + 1));
    }
    return expressions[index];
  }
}