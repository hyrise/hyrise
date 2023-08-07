#pragma once

#include "abstract_expression.hpp"
#include "utils/make_bimap.hpp"

namespace hyrise {

/**
 * Supported window/aggregate functions. In addition to the default SQL functions (e.g., MIN(), MAX()), Hyrise
 * internally uses the ANY() function, which expects all values in the group to be equal and returns that value. In SQL
 * terms, this would be an additional, but unnecessary GROUP BY column. This function is only used by the optimizer in
 * case that all values of the group are known to be equal (see DependentGroupByReductionRule).
 */
enum class WindowFunction {
  Min,
  Max,
  Sum,
  Avg,
  Count,
  CountDistinct,
  StandardDeviationSample,
  Any,
  CumeDist,
  DenseRank,
  PercentRank,
  Rank,
  RowNumber
};

// Actual aggregate functions.
const auto aggregate_functions = std::unordered_set<WindowFunction>{WindowFunction::Min,
                                                                    WindowFunction::Max,
                                                                    WindowFunction::Sum,
                                                                    WindowFunction::Avg,
                                                                    WindowFunction::Count,
                                                                    WindowFunction::CountDistinct,
                                                                    WindowFunction::StandardDeviationSample,
                                                                    WindowFunction::Any};

std::ostream& operator<<(std::ostream& stream, const WindowFunction window_function);

const auto window_function_to_string = make_bimap<WindowFunction, std::string>({
    {WindowFunction::Min, "MIN"},
    {WindowFunction::Max, "MAX"},
    {WindowFunction::Sum, "SUM"},
    {WindowFunction::Avg, "AVG"},
    {WindowFunction::Count, "COUNT"},
    {WindowFunction::CountDistinct, "COUNT DISTINCT"},
    {WindowFunction::StandardDeviationSample, "STDDEV_SAMP"},
    {WindowFunction::Any, "ANY"},
    {WindowFunction::CumeDist, "CUME_DIST"},
    {WindowFunction::DenseRank, "DENSE_RANK"},
    {WindowFunction::PercentRank, "PERCENT_RANK"},
    {WindowFunction::Rank, "RANK"},
    {WindowFunction::RowNumber, "ROW_NUMBER"},
});

/**
 * SQL Window/aggregate functions with optionally defined window. Window functions are a superset of aggregate
 * functions: Each aggregate function is a window function when defining a window. Thus, we keep it simple and use
 *  aggregate functions as window functions without a window and a certain function type.
 */
class WindowFunctionExpression : public AbstractExpression {
 public:
  WindowFunctionExpression(const WindowFunction init_window_function,
                           const std::shared_ptr<AbstractExpression>& argument,
                           const std::shared_ptr<AbstractExpression>& window = nullptr);

  std::shared_ptr<AbstractExpression> argument() const;

  std::shared_ptr<AbstractExpression> window() const;

  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  static bool is_count_star(const AbstractExpression& expression);

  const WindowFunction window_function;

 protected:
  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;

  bool _has_window{false};
};

}  // namespace hyrise
