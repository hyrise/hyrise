#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right,
                                           optional<std::pair<std::string, std::string>> column_names,
                                           const std::string &op, const JoinMode mode, const std::string &prefix_left,
                                           const std::string &prefix_right)
    : AbstractReadOnlyOperator(left, right),
      _op(op),
      _mode(mode),
      _prefix_left(prefix_left),
      _prefix_right(prefix_right),
      _column_names(column_names) {}

}  // namespace opossum
