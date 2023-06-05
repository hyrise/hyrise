#pragma once

#include "abstract_expression.hpp"

namespace hyrise {

enum class FrameBoundType { Preceding, Following, CurrentRow };
std::ostream& operator<<(std::ostream& stream, const FrameBoundType frame_bound_type);

struct FrameBound {
  FrameBound(const uint64_t init_offset, const FrameBoundType init_type, const bool init_unbounded);

  FrameBound() = default;
  FrameBound(const FrameBound&) = default;
  FrameBound(FrameBound&&) = default;

  FrameBound& operator=(const FrameBound&) = default;
  FrameBound& operator=(FrameBound&&) = default;

  bool operator==(const FrameBound& rhs) const;

  size_t hash() const;

  std::string description() const;

  uint64_t offset{0};
  FrameBoundType type{FrameBoundType::Preceding};
  bool unbounded{false};
};

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_bound);

enum class FrameType { Rows, Range, Groups };
std::ostream& operator<<(std::ostream& stream, const FrameType frame_type);

struct FrameDescription : private Noncopyable {
  FrameDescription(const FrameType init_type, const FrameBound& init_start, const FrameBound& init_end);

  bool operator==(const FrameDescription& rhs) const;
  std::string description() const;

  size_t hash() const;
  std::unique_ptr<FrameDescription> deep_copy() const;

  const FrameType type;
  const FrameBound start;
  const FrameBound end;
};

std::ostream& operator<<(std::ostream& stream, const FrameDescription& frame_description);

class WindowExpression : public AbstractExpression {
 public:
  WindowExpression(const std::vector<std::shared_ptr<AbstractExpression>>& partition_by_expressions,
                   const std::vector<std::shared_ptr<AbstractExpression>>& order_by_expressions,
                   const std::vector<SortMode>& init_sort_modes,
                   std::unique_ptr<FrameDescription> init_frame_description);

  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  std::vector<SortMode> sort_modes;
  std::unique_ptr<FrameDescription> frame_description;
  size_t order_by_expressions_begin_idx{0};

 protected:
  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;
};

}  // namespace hyrise
