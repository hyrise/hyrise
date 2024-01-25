#pragma once

#include "abstract_expression.hpp"

namespace hyrise {

enum class FrameBoundType { Preceding, Following, CurrentRow };
std::ostream& operator<<(std::ostream& stream, const FrameBoundType frame_bound_type);

struct FrameBound {
  FrameBound(const size_t init_offset, const FrameBoundType init_type, const bool init_unbounded);

  bool operator==(const FrameBound& rhs) const;

  size_t hash() const;

  std::string description() const;

  size_t offset{0};
  FrameBoundType type{FrameBoundType::Preceding};
  bool unbounded{false};
};

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_bound);

enum class FrameType { Rows, Range, Groups };
std::ostream& operator<<(std::ostream& stream, const FrameType frame_type);

struct FrameDescription {
  FrameDescription(const FrameType init_type, const FrameBound& init_start, const FrameBound& init_end);

  bool operator==(const FrameDescription& rhs) const;
  std::string description() const;

  size_t hash() const;
  std::unique_ptr<FrameDescription> deep_copy() const;

  FrameType type;
  FrameBound start;
  FrameBound end;
};

std::ostream& operator<<(std::ostream& stream, const FrameDescription& frame_description);

/**
 * Representation of a window used for SQL:2003 window functions (see window_function_expression.hpp). Windows define
 * how the window function is applied. If there are PARTITION BY expressions, the function is applied tuples in
 * different partitions independently. ORDER BY expressions define in which order the function is applied within the
 * same partition. The frame defines the actual sliding window of tuples used as input of the window function.
 */
class WindowExpression : public AbstractExpression {
 public:
  WindowExpression(std::vector<std::shared_ptr<AbstractExpression>>&& partition_by_expressions,
                   std::vector<std::shared_ptr<AbstractExpression>>&& order_by_expressions,
                   std::vector<SortMode>&& init_sort_modes, FrameDescription&& init_frame_description);

  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  std::vector<SortMode> sort_modes;
  FrameDescription frame_description;
  size_t order_by_expressions_begin_idx{0};

 protected:
  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;
};

}  // namespace hyrise
