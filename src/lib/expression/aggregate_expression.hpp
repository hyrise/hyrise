#pragma once

#include "abstract_expression.hpp"
#include "utils/make_bimap.hpp"

namespace hyrise {

/**
 * Supported aggregate functions. In addition to the default SQL functions (e.g., MIN(), MAX()), Hyrise internally uses
 * the ANY() function, which expects all values in the group to be equal and returns that value. In SQL terms, this
 * would be an additional, but unnecessary GROUP BY column. This function is only used by the optimizer in case that
 * all values of the group are known to be equal (see DependentGroupByReductionRule).
 */
enum class AggregateFunction { Min, Max, Sum, Avg, Count, CountDistinct, StandardDeviationSample, Any };
std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function);

// TODO: change to WindowFunctionExpression
enum class WindowFunction {
  Min,
  Max,
  Sum,
  Avg,
  Count,
  CountDistinct,
  StandardDeviationSample,
  CumeDist,
  DenseRank,
  PercentRank,
  Rank,
  RowNumber
};
std::ostream& operator<<(std::ostream& stream, const WindowFunction window_function);

enum class FrameBoundType { Preceding, Following, CurrentRow };
std::ostream& operator<<(std::ostream& stream, const FrameBoundType frame_bound_type);

struct FrameBound {
  FrameBound(const uint64_t offset, const FrameBoundType type, const bool unbounded);

  bool operator==(const FrameBound& rhs);

  const int64_t offset;
  const FrameBoundType type;
  const bool unbounded;
};

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_bound);

enum class FrameType { Rows, Range, Groups };
std::ostream& operator<<(std::ostream& stream, const FrameType frame_type);

struct FrameDescription {
  FrameDescription(const FrameType init_type, const std::unique_ptr<FrameBound> init_start,
                   const std::unique_ptr<FrameBound> init_end);

  bool operator==(const FrameDescription& rhs);

  const FrameType type;
  const std::unique_ptr<FrameBound> start;
  const std::unique_ptr<FrameBound> end;
};

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_description);

struct WindowDescription {
  WindowDescription(const std::vector<std::shared_ptr<AbstractExpression>>& init_partition_by_expressions,
                    const std::vector<std::shared_ptr<AbstractExpression>>& init_order_by_expressions,
                    const std::vector<SortMode>& init_sort_modes,
                    std::unique_ptr<FrameDescription> init_frame_description);

  bool operator==(const WindowDescription& rhs);

  std::vector<std::shared_ptr<AbstractExpression>> partition_by_expressions;
  std::vector<std::shared_ptr<AbstractExpression>> order_by_expressions;
  std::vector<SortMode> sort_modes;
  std::unique_ptr<FrameDescription> frame_description;
};

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_description);

const auto aggregate_function_to_string = make_bimap<AggregateFunction, std::string>({
    {AggregateFunction::Min, "MIN"},
    {AggregateFunction::Max, "MAX"},
    {AggregateFunction::Sum, "SUM"},
    {AggregateFunction::Avg, "AVG"},
    {AggregateFunction::Count, "COUNT"},
    {AggregateFunction::CountDistinct, "COUNT DISTINCT"},
    {AggregateFunction::StandardDeviationSample, "STDDEV_SAMP"},
    {AggregateFunction::Any, "ANY"},
});

const auto window_function_to_string = make_bimap<WindowFunction, std::string>({
    {WindowFunction::Min, "MIN"},
    {WindowFunction::Max, "MAX"},
    {WindowFunction::Sum, "SUM"},
    {WindowFunction::Avg, "AVG"},
    {WindowFunction::Count, "COUNT"},
    {WindowFunction::CountDistinct, "COUNT DISTINCT"},
    {WindowFunction::StandardDeviationSample, "STDDEV_SAMP"},
    {WindowFunction::CumeDist, "CUME_DIST"},
    {WindowFunction::DenseRank, "DENSE_RANK"},
    {WindowFunction::PercentRank, "PERCENT_RANK"},
    {WindowFunction::Rank, "RANK"},
    {WindowFunction::RowNumber, "ROW_NUMBER"},
});

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateFunction init_aggregate_function,
                      const std::shared_ptr<AbstractExpression>& argument);

  std::shared_ptr<AbstractExpression> argument() const;

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  static bool is_count_star(const AbstractExpression& expression);

  const AggregateFunction aggregate_function;

  std::unique_ptr<WindowDescription> window;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;
};

}  // namespace hyrise
