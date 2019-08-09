#include "testing_assert.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

bool check_lqp_tie(const std::shared_ptr<const AbstractLQPNode>& output, LQPInputSide input_side,
                   const std::shared_ptr<const AbstractLQPNode>& input) {
  auto outputs = input->outputs();
  for (const auto& output2 : outputs) {
    if (!output2) {
      return false;
    }
    if (output == output2 && output2->input(input_side) == input) {
      return true;
    }
  }

  return false;
}

}  // namespace opossum
