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

TEST_F(NumaUtilsTest, MergeNodePlacements) {
  auto optional_node_positions =
      std::vector<std::optional<NodeID>>{NodeID{0},    std::nullopt, NodeID{2},    NodeID{3}, NodeID{4},
                                         std::nullopt, std::nullopt, std::nullopt, NodeID{8}, NodeID{9}};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();

  auto addTask = [&](NodeID node_id) {
    auto task = std::make_shared<JobTask>([]() { return nullptr; });
    task->set_node_id(node_id);
    tasks.emplace_back(std::move(task));
  };

  addTask(NodeID{1});
  addTask(NodeID{5});
  addTask(NodeID{6});
  addTask(NodeID{7});

  auto node_placements = std::vector<NodeID>();
  numa_utils::merge_node_placements(node_placements, tasks, optional_node_positions);

  const auto expected_placements = std::vector<NodeID>{NodeID{0}, NodeID{1}, NodeID{2}, NodeID{3}, NodeID{4},
                                                       NodeID{5}, NodeID{6}, NodeID{7}, NodeID{8}, NodeID{9}};
  EXPECT_EQ(node_placements, expected_placements);
}

}  // namespace hyrise