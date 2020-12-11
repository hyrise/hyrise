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
  }

  template<typename COLUMN_TYPE>
  std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>
  DipsPruningRule::get_not_pruned_range_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) const {
    /* For every non pruned chunk, return its ranges for the given attribute (segment) */
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> ranges;

    auto pruned_chunks_ids = table_node->pruned_chunk_ids();                          //const std::vector<ChunkID>&
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);

    for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index){
      if(std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end())
      {
        auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
        const auto segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<COLUMN_TYPE>>(chunk_statistic);

        if constexpr (std::is_arithmetic_v<COLUMN_TYPE>) {
          if (segment_statistics->range_filter) {                       // false if all values in the chunk are NULL
            ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(chunk_index, segment_statistics->range_filter->ranges));
          } else {
            ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(chunk_index, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>()));
            // Note: if we don't do it, we assume, the chunk has been already pruned -> error
            continue;  
          }

          // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
          DebugAssert(!segment_statistics->min_max_filter,
                      "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
        }

        if (segment_statistics->min_max_filter) {
          ranges.insert(std::pair<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>(
            chunk_index, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>(
              {std::pair<COLUMN_TYPE, COLUMN_TYPE>(segment_statistics->min_max_filter->min,
                                                       segment_statistics->min_max_filter->max)}
              )
            )
          );
        }
      }
    }

    return ranges;
  }


  template<typename COLUMN_TYPE>
  bool DipsPruningRule::range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a, std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const {
    return (range_b.first <= range_a.first && range_b.second >= range_a.second) ||
            (range_b.first >= range_a.first && range_b.first <= range_a.second) || 
            (range_b.second >= range_a.first && range_b.second <= range_a.second);
  } 


  template<typename COLUMN_TYPE>
  std::set<ChunkID> DipsPruningRule::calculate_pruned_chunks(
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> base_chunk_ranges,
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> partner_chunk_ranges
  ) const
  {
    /* Calculate the chunks ids of the partner table which can be pruned (based on base_chunk_ranges) */
    std::set<ChunkID> pruned_chunk_ids;
    
    for (auto const& [partner_chunk_id, partner_ranges] : partner_chunk_ranges) {
      bool can_be_pruned = true;

      for (auto partner_range : partner_ranges){
        if(!can_be_pruned) break;
        for (auto const& [base_chunk_id, base_ranges] : base_chunk_ranges) {
          if(!can_be_pruned) break;
          for (auto base_range : base_ranges){
            if (range_intersect<COLUMN_TYPE>(partner_range, base_range)){
              can_be_pruned = false;
              break;
            }
          }
        }
      }
      if(can_be_pruned){
        pruned_chunk_ids.insert(partner_chunk_id);
      }
    }

    return pruned_chunk_ids;
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

}