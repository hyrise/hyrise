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
          
      //std::unique_ptr<ChunkPruningRule> chunk_pruning = std::make_unique<ChunkPruningRule>();

      // std::cout << "Predicates  for node: " << join_node << std::endl;
      for (auto predicate : join_predicates) {
        // std::cout << "  " << predicate->description() << std::endl;

        std::shared_ptr<BinaryPredicateExpression> binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
        auto left_operand = binary_predicate->left_operand();
        auto right_operand = binary_predicate->right_operand();
        //std::cout << "    " << left_operand->description() << std::endl;
        //std::cout << "    " << right_operand->description() << std::endl;

        //TODO: make sure JOIN node has parents

        auto left_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(left_operand);
        auto right_lqp = std::dynamic_pointer_cast<LQPColumnExpression>(right_operand);

        if (!left_lqp || !right_lqp) {
          return;
        }

        std::shared_ptr<StoredTableNode> left_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(left_lqp->original_node.lock()));
        std::shared_ptr<StoredTableNode> right_stored_table_node = std::const_pointer_cast<StoredTableNode>(std::dynamic_pointer_cast<const StoredTableNode>(right_lqp->original_node.lock()));
        int number_of_pruned_left = left_stored_table_node->pruned_chunk_ids().size();
        int number_of_pruned_right = right_stored_table_node->pruned_chunk_ids().size();

        if (!left_stored_table_node || !right_stored_table_node) {
          return;
        }

        // LEFT -> RIGHT
        
        //auto left_predicate_node = get_pruned_attribute_statistics(left_stored_table_node, left_lqp->original_column_id, right_lqp);
        dips_pruning(left_stored_table_node, left_lqp->original_column_id, right_stored_table_node, right_lqp->original_column_id);
        
        // auto left_parent_node = (right_stored_table_node->outputs())[0];

        // if (left_parent_node->left_input() == right_stored_table_node) {
        //   lqp_insert_node(left_parent_node, LQPInputSide::Left, left_predicate_node);
        // } 
        // else {
        //   lqp_insert_node(left_parent_node, LQPInputSide::Right, left_predicate_node);
        // }


        //chunk_pruning->apply_to(left_predicate_node, 1);



        // RIGHT -> LEFT
        dips_pruning(right_stored_table_node, right_lqp->original_column_id, left_stored_table_node, left_lqp->original_column_id);
        // auto right_parent_node = (left_stored_table_node->outputs())[0];



        // if (right_parent_node->left_input() == left_stored_table_node) {
        //     lqp_insert_node(right_parent_node, LQPInputSide::Left, right_predicate_node);
        // } 
        // else {
        //   lqp_insert_node(right_parent_node, LQPInputSide::Right, right_predicate_node);
        // }

        //chunk_pruning->apply_to(right_predicate_node, 1);

        std::cout << "Prune on " << left_stored_table_node->table_name << " Before: " << number_of_pruned_left << " After " << left_stored_table_node->pruned_chunk_ids().size() << std::endl;
        std::cout << "Prune on " << right_stored_table_node->table_name << " Before: " << number_of_pruned_right << " After " << right_stored_table_node->pruned_chunk_ids().size() << std::endl;
      
      }

      // auto left_operand = (std::shared_ptr<BinaryPredicateExpression>) predicate->left_operand()
    }
  }

  // std::shared_ptr<PredicateNode> DipsCreationRule::get_pruned_attribute_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<LQPColumnExpression> join_partner) const {
  //   auto pruned_chunks_ids = table_node->pruned_chunk_ids(); //const std::vector<ChunkID>&
  //   auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);
  //   std::vector<std::shared_ptr<AbstractExpression>> predicate_expressions;



  //   // std::cout << "Table: " << table_node->table_name << "and join Partner: " << join_partner->description(AbstractExpression::DescriptionMode::Detailed) << std::endl;

  //   for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index){
       
  //     if(std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end())
  //     {
  //       auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
  //       resolve_data_type(chunk_statistic->data_type, [&](const auto data_type_t) {
  //         using ColumnDataType = typename decltype(data_type_t)::type;

  //         const auto segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(chunk_statistic);

  //         // Range filters are only available for arithmetic (non-string) types.
  //         if constexpr (std::is_arithmetic_v<ColumnDataType>) {
  //           if (segment_statistics->range_filter) {
  //             for (const auto range: segment_statistics->range_filter->ranges)  {
  //               // std::cout << "    Chunk Index " << chunk_index << std::endl;
  //               // std::cout << "        Range First " << range.first << " Range Second " << range.second << std::endl;
  //               auto new_expression = expression_functional::between_inclusive_(join_partner, range.first, range.second);
  //               // auto new_predicate = PredicateNode::make(expression_functional::between_inclusive_(join_partner, range.first, range.second));
  //               predicate_expressions.push_back(new_expression);
  //             }
  //           }
  //           // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
  //           DebugAssert(!segment_statistics->min_max_filter,
  //                       "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
  //         }

  //       if (segment_statistics->min_max_filter) {
  //         auto new_expression = expression_functional::between_inclusive_(join_partner, segment_statistics->min_max_filter->min, segment_statistics->min_max_filter->max);
  //               // auto new_predicate = PredicateNode::make(expression_functional::between_inclusive_(join_partner, range.first, range.second));
  //         predicate_expressions.push_back(new_expression);
  //       }
  //       });
  //     } 
  //   }
  //   // TODO:
  //   // iterate over unpruned chunks
  //   // take chunk statistics
  //   // adjust predicate

  //   if (predicate_expressions.size() == 0) {
  //     return PredicateNode::make(expression_functional::or_(true, true));
  //   }
  //   auto collected_expression = build_logical_expression(predicate_expressions, 0);
  //   //return PredicateNode::make(collected_expression);
  //   return PredicateNode::make(expression_functional::or_(predicate_expressions[0], predicate_expressions[1]));
  // }

  // std::shared_ptr<AbstractExpression> DipsCreationRule::build_logical_expression(std::vector<std::shared_ptr<AbstractExpression>> expressions, long unsigned int index) const {
  //   if (index < expressions.size() - 1){
  //     return expression_functional::and_(expressions[index], build_logical_expression(expressions, index + 1));
  //   }
  //   return expressions[index];
  // }

  //----------------------------------------------------------------------------------------

  template<typename COLUMN_TYPE>
  std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>>
  DipsCreationRule::get_not_pruned_range_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id) const {
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
        } else {
          std::cout << "      Is not arithmetic" << std::endl;
          return ranges;
        }

        // if (segment_statistics->min_max_filter) {
        //   //chunk_ranges.push_back(std::make_pair<ColumnDataType, ColumnDataType>(segment_statistics->min_max_filter->min, segment_statistics->min_max_filter->max));
        
        // }
      }
    }

    return ranges;
  }


  template<typename COLUMN_TYPE>
  bool DipsCreationRule::range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a, std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const {
    return (range_b.first <= range_a.first && range_b.second >= range_a.second) ||
            (range_b.first >= range_a.first && range_b.first <= range_a.second) || 
            (range_b.second >= range_a.first && range_b.second <= range_a.second);
    // return !(
    //   (range_a.first < range_b.first && range_a.second < range_b.first)
    //   ||
    //   (range_a.first > range_b.second && range_a.second > range_b.second)
    // );
  } 


  template<typename COLUMN_TYPE>
  std::set<ChunkID> DipsCreationRule::calculate_pruned_chunks(
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

  void DipsCreationRule::extend_pruned_chunks( std::shared_ptr<StoredTableNode> table_node, std::set<ChunkID> pruned_chunk_ids) const
  {
    const auto& already_pruned_chunk_ids = table_node->pruned_chunk_ids();
    // std::cout << "Length of pruned chunks ids: " << pruned_chunk_ids.size() << '\n';
    
    if (!already_pruned_chunk_ids.empty()) {
      std::vector<ChunkID> union_values;
      std::set_union(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), pruned_chunk_ids.begin(),
                          pruned_chunk_ids.end(), std::back_inserter(union_values));
      table_node->set_pruned_chunk_ids(union_values);
    } else {
      table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
    }
  }

//------------------------------------------------------------------


  void DipsCreationRule::dips_pruning(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<StoredTableNode> join_partner_table_node, ColumnID join_partner_column_id) const {
    // auto join_parner_table = Hyrise::get().storage_manager.get_table(join_partner_table_node->table_name);
    // auto pruned_chunks_ids = table_node->pruned_chunk_ids(); //const std::vector<ChunkID>&
    auto table = Hyrise::get().storage_manager.get_table(table_node->table_name);


    resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      auto base_ranges = get_not_pruned_range_statistics<ColumnDataType>(table_node, column_id);
      auto partner_ranges = get_not_pruned_range_statistics<ColumnDataType>(join_partner_table_node, join_partner_column_id);
      auto pruned_chunks = calculate_pruned_chunks<ColumnDataType>(base_ranges, partner_ranges);
      extend_pruned_chunks(join_partner_table_node, pruned_chunks);
      
    //   std::vector<std::vector<std::pair<ColumnDataType, ColumnDataType>>> ranges;

    //   for (ChunkID chunk_index = ChunkID{0}; chunk_index < table->chunk_count(); ++chunk_index){
        
    //     std::vector<std::pair<ColumnDataType, ColumnDataType>> chunk_ranges;

    //     if(std::find(pruned_chunks_ids.begin(), pruned_chunks_ids.end(), chunk_index) == pruned_chunks_ids.end())
    //     {       
    //       auto chunk_statistic = (*table->get_chunk(chunk_index)->pruning_statistics())[column_id];
    //       const auto segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(chunk_statistic);

    //       // Range filters are only available for arithmetic (non-string) types.
    //       if constexpr (std::is_arithmetic_v<ColumnDataType>) {
    //         if (segment_statistics->range_filter) {         // false if all values in the chunk are NULL
    //           ranges.push_back(segment_statistics->range_filter->ranges);
    //         } else {
    //           std::cout << "    has no range filter\n";
    //           std::cout << "        table: " << table_node->table_name << "\n";
    //           std::cout << "        chunk: " << chunk_index << "\n";
    //           std::cout << "        column_id: " << column_id << "\n";
              
    //           return;   // WARNING: if a range filter doesnt exist, we can ignore the chunk, but the access in ranges-vector will be shifted 
    //         }

    //         // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
    //         DebugAssert(!segment_statistics->min_max_filter,
    //                     "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
    //       } else {
    //         std::cout << "      Is not arithmetic" << std::endl;
    //         return;
    //       }

    //       if (segment_statistics->min_max_filter) {
    //         //chunk_ranges.push_back(std::make_pair<ColumnDataType, ColumnDataType>(segment_statistics->min_max_filter->min, segment_statistics->min_max_filter->max));
          
    //       }
    //     }
    //   }

    // //-------------------------------------------------
    //   std::cout << "    Number of pruned chunks IDs for table:" << table_node->table_name << " is: " << pruned_chunks_ids.size() <<  '\n';
    //   for (auto chunk_id : pruned_chunks_ids){
    //     std::cout << "        " << chunk_id << "\n";
    //   }

    //   std::cout << "    Ranges info from table: " << table_node->table_name << '\n';
    //   for (uint i = 0; i <  ranges.size(); ++i){
    //     std::cout << "        Entry: " << i << '\n';
    //     for (uint j = 0; j <  ranges[i].size(); ++j){
    //       std::cout << "            {"<< ranges[i][j].first << " -> " <<  ranges[i][j].second << "} " << '\n';
    //     }
    //   }

    // //------------------------------------------------
    //   std::set<ChunkID> pruned_chunk_ids;
      
    //   auto join_partner_pruned_chunks_ids = join_partner_table_node->pruned_chunk_ids();

    //   for (ChunkID chunk_index = ChunkID{0}; chunk_index < join_parner_table->chunk_count(); ++chunk_index){
    //     if(std::find(join_partner_pruned_chunks_ids.begin(), join_partner_pruned_chunks_ids.end(), chunk_index) == join_partner_pruned_chunks_ids.end()){
    //       bool can_be_pruned = true;

    //       auto join_partner_chunk_statistic = (*join_parner_table->get_chunk(chunk_index)->pruning_statistics())[join_partner_column_id];
    //       const auto join_partner_segment_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(join_partner_chunk_statistic);

    //       // Range filters are only available for arithmetic (non-string) types.
    //       if constexpr (std::is_arithmetic_v<ColumnDataType>) {
    //         if (join_partner_segment_statistics->range_filter) {
    //           std::cout << "Reached Range Filter of Join Partner for Chunk ID " << chunk_index << " With range filter of size " << join_partner_segment_statistics->range_filter->ranges.size() << std::endl;
    //           std::cout << "We compare with " << ranges.size() << " chunk ranges" << std::endl;
    //           for(auto join_partner_range : join_partner_segment_statistics->range_filter->ranges) {
    //             if (!can_be_pruned) {
    //               break;
    //             }
    //             for (auto chunk_ranges : ranges){
    //               if (!can_be_pruned) {
    //                 break;
    //               }
    //               for (auto range : chunk_ranges) {
    //                 //if( 
    //                 //    !(
    //                //       (range.first < join_partner_range.first && range.second < join_partner_range.first)
    //                 //      ||
    //                //       (range.first > join_partner_range.second && range.second > join_partner_range.second)
    //               //      )
    //               //    ) {
    //               //    can_be_pruned = false;
    //              //     break;
    //              //   }

    //                 if((join_partner_range.first <= range.first && join_partner_range.second >= range.second) ||
    //                    (join_partner_range.first >= range.first && join_partner_range.first <= range.second) || 
    //                    (join_partner_range.second >= range.first && join_partner_range.second <= range.second)) {
    //                   can_be_pruned = false;
    //                   break;
    //                 }
    //               }
    //             }
    //           }
    //         }
    //         // RangeFilters contain all the information stored in a MinMaxFilter. There is no point in having both.
    //         DebugAssert(!join_partner_segment_statistics->min_max_filter,
    //                     "Segment should not have a MinMaxFilter and a RangeFilter at the same time");
    //       }

    //       //
    //       if(can_be_pruned){
    //         pruned_chunk_ids.insert(chunk_index);
    //       }
    //     }
    //   }



    //   const auto& already_pruned_chunk_ids = join_partner_table_node->pruned_chunk_ids();
    //   std::cout << "Length of pruned chunks ids: " << pruned_chunk_ids.size() << '\n';
      
    //   if (!already_pruned_chunk_ids.empty()) {
    //     std::vector<ChunkID> union_values;
    //     std::set_union(already_pruned_chunk_ids.begin(), already_pruned_chunk_ids.end(), pruned_chunk_ids.begin(),
    //                         pruned_chunk_ids.end(), std::back_inserter(union_values));
    //     join_partner_table_node->set_pruned_chunk_ids(union_values);
    //   } else {
    //     join_partner_table_node->set_pruned_chunk_ids(std::vector<ChunkID>(pruned_chunk_ids.begin(), pruned_chunk_ids.end()));
    //   }
    
    });
  } 

}