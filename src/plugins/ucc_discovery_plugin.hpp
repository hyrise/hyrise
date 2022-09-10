#pragma once

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

/*
 *  This plugin implements unary Unique Column Combination (UCC) discovery based on previously executed LQPs.
 *  Not all columns encountered in these LQPs are automatically considered for the UCC validation process.
 *  Instead, columns are only validated/invalidated as UCCs if their being a UCC could've helped to optimize their LQP.
 */
class UCCCandidate {
 public:
  UCCCandidate(const std::string& table_name, const ColumnID column_id)
      : _table_name(table_name), _column_id(column_id) {}

  const std::string& table_name() const {
    return _table_name;
  }

  const ColumnID column_id() const {
    return _column_id;
  }

  friend bool operator==(const UCCCandidate& lhs, const UCCCandidate& rhs) {
    return (lhs.column_id() == rhs.column_id()) && (lhs.table_name() == rhs.table_name());
  }

 protected:
  const std::string _table_name;
  const ColumnID _column_id;
};

using UCCCandidates = std::unordered_set<UCCCandidate>;

class UccDiscoveryPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

 protected:
  friend class UccDiscoveryPluginTest;

  UCCCandidates identify_ucc_candidates() const;
  std::shared_ptr<std::vector<UCCCandidate>> generate_valid_candidates(
      std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate) const;

  void discover_uccs() const;

  std::unique_ptr<PausableLoopThread> _loop_thread_start;

 private:
  /**
  * Extracts columns as candidates for ucc validation from the aggregate node, that are used in groupby operations.
  */
  void _ucc_candidates_from_aggregate_node(std::shared_ptr<AbstractLQPNode> node, UCCCandidates& ucc_candidates) const;

  /**
  * Extracts columns as ucc validation candidates from a join node. 
  * Some criteria have to be fulfilled for this to be done:
  *   - The Node may only have one predicate.
  *   - This predicate must have the equals condition. This may be extended in the future to support other conditions.
  *   - The join must be either a left/right outer, semi or inner join. These cause one of the input sides to no longer
  *     be needed after the join has been completed and therefore allow for optimization by replacement.
  * In addition to the column corresponding to the removable side of the join, the removable input side LQP is iterated
  * and a column used in a PredicateNode may be added as a UCC candidate if the Predicate filters the same table that
  * contains the join column.
  */
  void _ucc_candidates_from_join_node(std::shared_ptr<AbstractLQPNode> node,  UCCCandidates& ucc_candidates) const;
};

}  // namespace hyrise

template <>
struct std::hash<hyrise::UCCCandidate> {
  std::size_t operator()(const hyrise::UCCCandidate& s) const noexcept {
    std::size_t h1 = std::hash<std::string>{}(s.table_name());
    std::size_t h2 = std::hash<hyrise::ColumnID>{}(s.column_id());
    return h1 ^ (h2 << 1);
  }
};
