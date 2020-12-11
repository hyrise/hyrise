#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"
#include "expression/abstract_expression.hpp"

#include "abstract_rule.hpp"


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
    std::shared_ptr<PredicateNode> get_pruned_attribute_statistics(
      const std::shared_ptr<const StoredTableNode> table_node, 
      ColumnID column_id, 
      std::shared_ptr<LQPColumnExpression> join_partner) const;
    
    void dips_pruning(
      const std::shared_ptr<const StoredTableNode> table_node, 
      ColumnID column_id, 
      std::shared_ptr<StoredTableNode> join_partner_table_node, 
      ColumnID join_partner_column_id) const;

    void extend_pruned_chunks( std::shared_ptr<StoredTableNode> table_node, std::set<ChunkID> pruned_chunk_ids) const;

    template<typename COLUMN_TYPE>
    std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> get_not_pruned_range_statistics(
      const std::shared_ptr<const StoredTableNode> table_node, 
      ColumnID column_id) const;
    
    template<typename COLUMN_TYPE>
    bool range_intersect(std::pair<COLUMN_TYPE, COLUMN_TYPE> range_a, std::pair<COLUMN_TYPE, COLUMN_TYPE> range_b) const;
    
    template<typename COLUMN_TYPE>
    std::set<ChunkID> calculate_pruned_chunks(
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> base_chunk_ranges,
      std::map<ChunkID, std::vector<std::pair<COLUMN_TYPE, COLUMN_TYPE>>> partner_chunk_ranges
    ) const; 
  };

}  // namespace opossum
