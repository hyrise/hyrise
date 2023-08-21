#include "base_test.hpp"

#include "types.hpp"
#include "utils/numa_utils.hpp"

namespace hyrise {

class NumaUtilsTest : public BaseTest {};

TEST_F(NumaUtilsTest, MakeNodePriorityMatrix) {
  const auto distance_matrix = numa_utils::DistanceMatrix{
      {1, 2, 3},
      {3, 2, 1},
      {2, 3, 1},
  };

  const auto priority_matrix = numa_utils::make_node_priority_matrix(distance_matrix);

  const auto expected_priority_matrix = numa_utils::NodePriorityMatrix{
      {NodeID{0}, NodeID{1}, NodeID{2}}, {NodeID{2}, NodeID{1}, NodeID{0}}, {NodeID{2}, NodeID{0}, NodeID{1}}};

  EXPECT_EQ(priority_matrix, expected_priority_matrix);
}

}  // namespace hyrise