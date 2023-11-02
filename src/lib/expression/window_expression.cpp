#include "window_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const FrameBoundType frame_bound_type) {
  if (frame_bound_type == FrameBoundType::CurrentRow) {
    stream << "CURRENT ROW";
    return stream;
  }

  auto type_str = std::string{magic_enum::enum_name(frame_bound_type)};
  std::transform(type_str.cbegin(), type_str.cend(), type_str.begin(),
                 [](const auto character) { return std::toupper(character); });
  stream << type_str;
  return stream;
}

FrameBound::FrameBound(const size_t init_offset, const FrameBoundType init_type, const bool init_unbounded)
    : offset{init_offset}, type{init_type}, unbounded{init_unbounded} {}

bool FrameBound::operator==(const FrameBound& rhs) const {
  return offset == rhs.offset && type == rhs.type && unbounded == rhs.unbounded;
}

size_t FrameBound::hash() const {
  auto hash_value = offset;
  boost::hash_combine(hash_value, static_cast<size_t>(type));
  boost::hash_combine(hash_value, boost::hash_value(unbounded));
  return hash_value;
}

std::string FrameBound::description() const {
  auto stream = std::stringstream{};

  if (type != FrameBoundType::CurrentRow) {
    if (unbounded) {
      stream << "UNBOUNDED";
    } else {
      stream << offset;
    }
    stream << " ";
  }

  stream << type;
  return stream.str();
}

std::ostream& operator<<(std::ostream& stream, const FrameBound& frame_bound) {
  stream << frame_bound.description();
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const FrameType frame_type) {
  auto type_str = std::string{magic_enum::enum_name(frame_type)};
  std::transform(type_str.cbegin(), type_str.cend(), type_str.begin(),
                 [](const auto character) { return std::toupper(character); });
  stream << type_str;
  return stream;
}

FrameDescription::FrameDescription(const FrameType init_type, const FrameBound& init_start, const FrameBound& init_end)
    : type{init_type}, start{init_start}, end{init_end} {}

bool FrameDescription::operator==(const FrameDescription& rhs) const {
  return type == rhs.type && start == rhs.start && end == rhs.end;
}

std::string FrameDescription::description() const {
  auto stream = std::stringstream{};
  stream << type << " BETWEEN " << start << " AND " << end;
  return stream.str();
}

size_t FrameDescription::hash() const {
  auto hash_value = static_cast<size_t>(type);
  boost::hash_combine(hash_value, start.hash());
  boost::hash_combine(hash_value, end.hash());
  return hash_value;
}

std::ostream& operator<<(std::ostream& stream, const FrameDescription& frame_description) {
  stream << frame_description.description();
  return stream;
}

WindowExpression::WindowExpression(std::vector<std::shared_ptr<AbstractExpression>>&& partition_by_expressions,
                                   std::vector<std::shared_ptr<AbstractExpression>>&& order_by_expressions,
                                   std::vector<SortMode>&& init_sort_modes, FrameDescription&& init_frame_description)
    : AbstractExpression{ExpressionType::Window, {/* Expressions added below. */}},
      sort_modes{init_sort_modes},
      frame_description{init_frame_description},
      order_by_expressions_begin_idx{partition_by_expressions.size()} {
  const auto order_by_expression_count = order_by_expressions.size();
  Assert(order_by_expression_count == sort_modes.size(), "Passed sort modes do not match ORDER BY expressions.");

  arguments.resize(order_by_expressions_begin_idx + order_by_expression_count);
  std::copy(partition_by_expressions.begin(), partition_by_expressions.end(), arguments.begin());
  std::copy(order_by_expressions.begin(), order_by_expressions.end(),
            arguments.begin() + order_by_expressions_begin_idx);
}

std::shared_ptr<AbstractExpression> WindowExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto expression_count = arguments.size();
  const auto order_by_expression_count = expression_count - order_by_expressions_begin_idx;
  auto partition_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(order_by_expressions_begin_idx);
  auto order_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(order_by_expression_count);

  for (auto expression_idx = size_t{0}; expression_idx < order_by_expressions_begin_idx; ++expression_idx) {
    partition_by_expressions[expression_idx] = arguments[expression_idx]->deep_copy(copied_ops);
  }

  for (auto expression_idx = size_t{0}; expression_idx < order_by_expression_count; ++expression_idx) {
    order_by_expressions[expression_idx] =
        arguments[order_by_expressions_begin_idx + expression_idx]->deep_copy(copied_ops);
  }
  auto frame_description_copy = frame_description;
  auto sort_modes_copy = sort_modes;

  return std::make_shared<WindowExpression>(std::move(partition_by_expressions), std::move(order_by_expressions),
                                            std::move(sort_modes_copy), std::move(frame_description_copy));
}

std::string WindowExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};
  if (order_by_expressions_begin_idx > 0) {
    stream << "PARTITION BY " << arguments[0]->description(mode);

    for (auto expression_idx = size_t{1}; expression_idx < order_by_expressions_begin_idx; ++expression_idx) {
      stream << ", " << arguments[expression_idx]->description(mode);
    }

    stream << " ";
  }

  const auto expression_count = arguments.size();
  if (order_by_expressions_begin_idx < expression_count) {
    stream << "ORDER BY " << arguments[order_by_expressions_begin_idx]->description(mode) << " " << sort_modes[0];

    for (auto expression_idx = order_by_expressions_begin_idx + 1; expression_idx < expression_count;
         ++expression_idx) {
      stream << ", " << arguments[expression_idx]->description(mode) << " "
             << sort_modes[expression_idx - order_by_expressions_begin_idx];
    }

    stream << " ";
  }

  stream << frame_description;
  return stream.str();
}

DataType WindowExpression::data_type() const {
  Fail("WindowExpression must only be used as descriptor for WindowFunctionExpression.");
}

bool WindowExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const WindowExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& rhs = static_cast<const WindowExpression&>(expression);
  return sort_modes == rhs.sort_modes && frame_description == rhs.frame_description;
}

size_t WindowExpression::_shallow_hash() const {
  auto hash_value = sort_modes.size();
  for (const auto sort_mode : sort_modes) {
    boost::hash_combine(hash_value, static_cast<size_t>(sort_mode));
  }
  boost::hash_combine(hash_value, frame_description.hash());
  return hash_value;
}

bool WindowExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  Fail("WindowExpression must only be used as descriptor for WindowFunctionExpression.");
}

}  // namespace hyrise
