#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * A SQL View represented by an LQP.
 * When used in an SQL query, the SQLTranslator hooks a copy of this LQP into the LQP it creates
 */
class LQPView {
 public:
  LQPView(const std::shared_ptr<AbstractLQPNode>& lqp, const std::unordered_map<ColumnID, std::string>& column_names);

  std::shared_ptr<LQPView> deep_copy() const;
  bool deep_equals(const LQPView& other) const;

  const std::shared_ptr<AbstractLQPNode> lqp;
  const std::unordered_map<ColumnID, std::string> column_names;
};

}  // namespace opossum
