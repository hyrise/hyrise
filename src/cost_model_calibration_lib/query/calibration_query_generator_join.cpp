#include "calibration_query_generator_join.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const std::shared_ptr<JoinNode> CalibrationQueryGeneratorJoin::generate_join() { return {}; }

}  // namespace opossum
