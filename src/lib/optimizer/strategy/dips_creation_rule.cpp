#include "dips_creation_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"
#include <iostream>

namespace opossum {

  void DipsCreationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
    if (node->type != LQPNodeType::Join) {
      _apply_to_inputs(node);
      return;
    }

    const auto& join_node = static_cast<JoinNode&>(*node);
    const auto join_predicates = join_node.join_predicates();


    // left_column
    // right_column

    // iterate left_nodes
    // find stored table with left column
    // check_for prunes

    // if pruned:
    //   check_statistics
    //   create_dips
    //   save_dips_to_opposite_side
        
    std::cout << "Predicates  for node: " << join_node << std::endl;
    for (auto predicate : join_predicates) {
      std::cout << "  " << predicate->description() << std::endl;

      std::shared_ptr<BinaryPredicateExpression> binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
      auto left_operand = binary_predicate->left_operand();
      auto right_operand = binary_predicate->right_operand();
      std::cout << "    " << left_operand->description() << std::endl;
      std::cout << "    " << right_operand->description() << std::endl;

      auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
      auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);


      auto shared = left_lqp->original_node.lock();

      const std::shared_ptr<const StoredTableNode> node = std::dynamic_pointer_cast<const StoredTableNode>(shared);
      //for left -> right
      auto left_predicate_nodes = get_pruned_attribute_statistics(node, left_lqp->original_column_id, right_lqp);

      // auto left_operand = (std::shared_ptr<BinaryPredicateExpression>) predicate->left_operand()
    }

    _apply_to_inputs(node);
  }

  std::vector<std::shared_ptr<PredicateNode>> DipsCreationRule::get_pruned_attribute_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<LQPColumnExpression> join_partner) const {
    auto pruned_chunks_ids = table_node->pruned_chunk_ids(); //const std::vector<ChunkID>&
    auto table_statistics = table_node->table_statistics;   //std::shared_ptr<TableStatistics>

    std::shared_ptr<BaseAttributeStatistics> base_segment_statistics = table_statistics->column_statistics[column_id];

    std::vector<std::shared_ptr<PredicateNode>> predicate_nodes;

    // TODO:
    // iterate over unpruned chunks
    // take chunk statistics
    // adjust predicates

    // for (const auto pruned_chunk_id: pruned_chunk_ids) {

    resolve_data_type(base_segment_statistics->data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(base_segment_statistics);

      // Range filters are only available for arithmetic (non-string) types.
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        if (segment_statistics->range_filter) {
          // TODO: Create Predicate
          for (const auto range: segment_statistics->range_filter->ranges)  {
            auto new_predicate = PredicateNode::make(expression_functional::between_inclusive_(join_partner, range.first, range.second));
            predicate_nodes.push_back(new_predicate);
          }
        }
        // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
        DebugAssert(!segment_statistics->min_max_filter,
                    "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
      }

    if (segment_statistics->min_max_filter) {
      
    }
    });
    }

    return predicate_nodes;
  }  
}