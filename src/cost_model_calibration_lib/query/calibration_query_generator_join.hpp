#pragma once

#include "logical_query_plan/join_node.hpp"

namespace opossum {
class CalibrationQueryGeneratorJoin {
 public:
  static const std::shared_ptr<JoinNode> generate_join();

 private:
  CalibrationQueryGeneratorJoin() = default;
};

}  // namespace opossum
