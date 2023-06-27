#include "base_test.hpp"

#include "utils/segment_tree.hpp"

namespace hyrise {

class SegmentTreeTest : public BaseTest {
 public:
  static auto leaf_count(const auto& segment_tree) {
    return segment_tree.leaf_count;
  }

  static auto tree_size(const auto& segment_tree) {
    return segment_tree.tree.size();
  }

  SegmentTree<int, std::plus<>, 0> segment_tree{std::array{1, 2, 3}};
};

TEST_F(SegmentTreeTest, BinaryTreeIsFull) {
  EXPECT_EQ(leaf_count(segment_tree), 4);
  // 8 nodes, as index 0 is unused
  EXPECT_EQ(tree_size(segment_tree), 9);
}

TEST_F(SegmentTreeTest, PointQuery) {
  EXPECT_EQ(segment_tree.range_query({0, 1}), 1);
  EXPECT_EQ(segment_tree.range_query({1, 2}), 2);
  EXPECT_EQ(segment_tree.range_query({2, 3}), 3);
}

TEST_F(SegmentTreeTest, RangeQuery) {
  EXPECT_EQ(segment_tree.range_query({0, 2}), 3);
  EXPECT_EQ(segment_tree.range_query({1, 3}), 5);
  EXPECT_EQ(segment_tree.range_query({0, 3}), 6);
}

}  // namespace hyrise
