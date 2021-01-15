#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"
#include "expression/abstract_expression.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

#include "abstract_rule.hpp"

#include "dips_pruning_rule.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"
#include "hyrise.hpp"
#include <iostream>

namespace opossum {

  class AbstractLQPNode;
  /**
  * This rule determines which chunks can be pruned from table scans based on
  * the predicates present in the LQP and stores that information in the stored
  * table nodes.
  */
  class DipsPruningRule : public AbstractRule {
  public:
    void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  protected:
    void dips_pruning(
      const std::shared_ptr<const StoredTableNode> table_node, 
      ColumnID column_id, 
      std::shared_ptr<StoredTableNode> join_partner_table_node, 
      ColumnID join_partner_column_id) const;

    void extend_pruned_chunks( std::shared_ptr<StoredTableNode> table_node, std::set<ChunkID> pruned_chunk_ids) const;


    template<typename COLUMN_TYPE>
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>
    get_not_pruned_range_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) const {
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
    bool range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a, std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const {
      return !(
        (
          (range_a.first < range_b.first) && (range_a.second < range_b.first)
        )
        ||
        (
          (range_a.first > range_b.second) && (range_a.second > range_b.second)
        )
      );
    } 


    template<typename COLUMN_TYPE>
    std::set<ChunkID> calculate_pruned_chunks(
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
  };

}  // namespace opossum
