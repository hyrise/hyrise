#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace hyrise {

enum class IndexSide : uint8_t { Left, Right };

struct JoinConfiguration {
  JoinConfiguration() = delete;

  JoinConfiguration(JoinMode init_join_mode, PredicateCondition init_predicate_condition, DataType init_left_data_type,
                    DataType init_right_data_type, bool init_secondary_predicates,
                    std::optional<TableType> init_left_table_type = std::nullopt,
                    std::optional<TableType> init_right_table_type = std::nullopt,
                    std::optional<IndexSide> init_index_side = std::nullopt)
      : join_mode(init_join_mode),
        predicate_condition(init_predicate_condition),
        left_data_type(init_left_data_type),
        right_data_type(init_right_data_type),
        secondary_predicates(init_secondary_predicates),
        left_table_type(init_left_table_type),
        right_table_type(init_right_table_type),
        index_side(init_index_side) {}

  JoinMode join_mode;
  PredicateCondition predicate_condition;
  DataType left_data_type;
  DataType right_data_type;
  bool secondary_predicates;
  // Only for JoinIndex
  std::optional<TableType> left_table_type;
  std::optional<TableType> right_table_type;
  std::optional<IndexSide> index_side;
};

/**
 * Base class for predicated (i.e., non-cross) join operator implementations. Cross Joins are performed by the Product
 * operator.
 *
 * Find more information about joins in our Wiki: https://github.com/hyrise/hyrise/wiki/Hash-Join-Operator
 * We have decided against forwarding MVCC data in https://github.com/hyrise/hyrise/issues/409
 */
class AbstractJoinOperator : public AbstractReadOnlyOperator {
 public:
  AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                       const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                       const OperatorJoinPredicate& primary_predicate,
                       const std::vector<OperatorJoinPredicate>& secondary_predicates,
                       std::unique_ptr<AbstractOperatorPerformanceData> performance_data =
                           std::make_unique<OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps>>());

  JoinMode mode() const;

  const OperatorJoinPredicate& primary_predicate() const;
  const std::vector<OperatorJoinPredicate>& secondary_predicates() const;

  std::string description(DescriptionMode description_mode) const override;

 protected:
  const JoinMode _mode;
  const OperatorJoinPredicate _primary_predicate;
  const std::vector<OperatorJoinPredicate> _secondary_predicates;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<Table> _build_output_table(std::vector<std::shared_ptr<Chunk>>&& chunks,
                                             const TableType table_type = TableType::References) const;
};

}  // namespace hyrise
