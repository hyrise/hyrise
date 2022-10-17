#pragma once

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

/**
 * UccCandidate instances represent a column (referencing the table by name and the column by ID). They are used to
 * first collect all candidates for UCC validation before actually validating them in the UccDiscoveryPlugin.
 */
struct UccCandidate {
  UccCandidate(const std::string& init_table_name, const ColumnID init_column_id);

  bool operator==(const UccCandidate& other) const;
  bool operator!=(const UccCandidate& other) const;
  size_t hash() const;

  const std::string table_name;
  const ColumnID column_id;

 private:
  // Disable default constructor with uninitialized members.
  UccCandidate() = default;
};

using UccCandidates = std::unordered_set<UccCandidate>;

/**
 *  This plugin implements unary Unique Column Combination (UCC) discovery based on previously executed LQPs. Not all
 *  columns encountered in these LQPs are automatically considered for the UCC validation process. Instead, columns are
 *  only validated/invalidated as UCCs if their being a UCC could've helped to optimize their LQP.
 */
class UccDiscoveryPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

 protected:
  friend class UccDiscoveryPluginTest;

  /**
   * Takes a snapshot of the current LQP Cache. Iterates through the LQPs and tries to extract sensible columns as can-
   * didates for UCC validation from each of them. Columns are added as candidates if their being a UCC has the poten-
   * tial to help optimize their respective LQP.
   * 
   * Returns an unordered set of these candidates to be used in the UCC validation function.
   */
  UccCandidates _identify_ucc_candidates() const;

  /**
   * Iterates over the provided set of columns identified as candidates for a uniqueness validation. Validates those
   * that are not already known to be unique.
   */
  void _validate_ucc_candidates(const UccCandidates& ucc_candidates) const;

 private:
  /**
   * Checks whether individual DictionarySegments contain duplicates. This is an efficient operation as the check is
   * simply comparing the length of the dictionary to that of the attribute vector. This function can therefore be used
   * for an early-out before the more expensive cross-segment uniqueness check.
   */
  template <typename ColumnDataType>
  bool _dictionary_segments_contain_duplicates(std::shared_ptr<Table> table, ColumnID column_id) const;

  /**
   * Checks whether the given table contains only unique values by inserting all values into an unordered set. If for
   * any table segment the size of the set increases by less than the number of values in that segment, we know that
   * there must be a duplicate and return false. Otherwise, returns true.
   */
  template <typename ColumnDataType>
  bool _uniqueness_holds_across_segments(std::shared_ptr<Table> table, ColumnID column_id) const;

  /**
   * Extracts columns as candidates for ucc validation from the aggregate node, that are used in groupby operations.
   */
  void _ucc_candidates_from_aggregate_node(std::shared_ptr<AbstractLQPNode> node, UccCandidates& ucc_candidates) const;

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
  void _ucc_candidates_from_join_node(std::shared_ptr<AbstractLQPNode> node, UccCandidates& ucc_candidates) const;

  /**
   * Iterates through the LQP underneath the given root node.
   * If a PredicateNode is encountered, checks whether it is a binary predicate checking for column = value.
   * If the predicate column is from the same table as the passed column_candidate, adds both as ucc_candidates
   */
  void _ucc_candidates_from_removable_join_input(std::shared_ptr<AbstractLQPNode> root_node,
                                                 std::shared_ptr<LQPColumnExpression> column_candidate,
                                                 UccCandidates& ucc_candidates) const;
};

}  // namespace hyrise

namespace std {

/**
 * Hash function required to manage UccCandidates in an unordered set for deduplication.
 */
template <>
struct hash<hyrise::UccCandidate> {
  size_t operator()(const hyrise::UccCandidate& ucc_candidate) const;
};

}  // namespace std
