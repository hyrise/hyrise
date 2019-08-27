#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace opossum {

enum class IndexSide { Left, Right };

struct JoinConfiguration {
  JoinMode join_mode;
  PredicateCondition predicate_condition;
  DataType left_data_type;
  DataType right_data_type;
  bool secondary_predicates;
  // Only for JoinIndex
  std::optional<TableType> left_table_type{std::nullopt};
  std::optional<TableType> right_table_type{std::nullopt};
  std::optional<IndexSide> index_side{std::nullopt};
};

/**
 * Base class for predicated (i.e., non-cross) join operator implementations. Cross Joins are performed by the Product
 * operator.
 *
 * Find more information about joins in our Wiki: https://github.com/hyrise/hyrise/wiki/Operator-Join
 * We have decided against forwarding MVCC data in https://github.com/hyrise/hyrise/issues/409
 */
class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(
      const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
      const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
      const OperatorJoinPredicate& primary_predicate, const std::vector<OperatorJoinPredicate>& secondary_predicates,
      std::unique_ptr<OperatorPerformanceData> performance_data = std::make_unique<OperatorPerformanceData>());

  JoinMode mode() const;

  const OperatorJoinPredicate& primary_predicate() const;
  const std::vector<OperatorJoinPredicate>& secondary_predicates() const;

  const std::string description(DescriptionMode description_mode) const override;

 protected:
  const JoinMode _mode;
  const OperatorJoinPredicate _primary_predicate;
  const std::vector<OperatorJoinPredicate> _secondary_predicates;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<Table> _build_output_table(std::vector<std::shared_ptr<Chunk>>&& chunks,
                                             const TableType table_type = TableType::References) const;

  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in join_hash.hpp.
  class AbstractJoinOperatorImpl : public AbstractReadOnlyOperatorImpl {
   public:
    virtual ~AbstractJoinOperatorImpl() = default;
    virtual std::shared_ptr<const Table> _on_execute() = 0;
  };
};

}  // namespace opossum
