#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class MultiPredicateJoinNode : public EnableMakeForLQPNode<MultiPredicateJoinNode>, public AbstractLQPNode {

};

}  // namespace opossum
