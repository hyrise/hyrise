#include "base_test.hpp"
#include "logical_query_plan/data_dependencies/functional_dependency.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace hyrise {

class FunctionalDependencyTest : public BaseTest {
 public:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "mock_node_a");
    _a = _mock_node_a->get_column("a");
    _b = _mock_node_a->get_column("b");
    _c = _mock_node_a->get_column("c");

    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}}, "mock_node_b");
    _x = _mock_node_b->get_column("x");
    _y = _mock_node_b->get_column("y");
  }

 protected:
  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _x, _y;
};

TEST_F(FunctionalDependencyTest, Equals) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_b = FunctionalDependency({_a, _b}, {_c});

  // Equal.
  EXPECT_EQ(fd_a, FunctionalDependency({_a}, {_b, _c}));
  EXPECT_EQ(fd_a, FunctionalDependency({_a}, {_c, _b}));
  EXPECT_EQ(fd_a_b, FunctionalDependency({_a, _b}, {_c}));
  EXPECT_EQ(fd_a_b, FunctionalDependency({_b, _a}, {_c}));

  // Not Equal.
  EXPECT_NE(fd_a, FunctionalDependency({_a}, {_c}));
  EXPECT_NE(fd_a, FunctionalDependency({_a, _x}, {_b, _c}));
  EXPECT_NE(fd_a_b, FunctionalDependency({_a, _b}, {_c, _x}));
  EXPECT_NE(fd_a_b, FunctionalDependency({_a}, {_c}));
}

TEST_F(FunctionalDependencyTest, Hash) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_b = FunctionalDependency({_a, _b}, {_c});

  EXPECT_EQ(fd_a.hash(), FunctionalDependency({_a}, {_b, _c}).hash());
  EXPECT_EQ(fd_a.hash(), FunctionalDependency({_a}, {_b}).hash());
  EXPECT_EQ(fd_a.hash(), FunctionalDependency({_a}, {_x, _y}).hash());
  EXPECT_EQ(fd_a_b.hash(), FunctionalDependency({_a, _b}, {_c}).hash());
  EXPECT_EQ(fd_a_b.hash(), FunctionalDependency({_b, _a}, {_c}).hash());
  EXPECT_EQ(fd_a_b.hash(), FunctionalDependency({_a, _b}, {_c, _x}).hash());
  EXPECT_EQ(fd_a_b.hash(), FunctionalDependency({_a, _b}, {_x}).hash());
}

TEST_F(FunctionalDependencyTest, ToStream) {
  auto stream = std::stringstream{};

  stream << FunctionalDependency{{_a}, {_b}};
  EXPECT_EQ(stream.str(), "{a} => {b}");
  stream.str("");

  stream << FunctionalDependency{{_a}, {_b, _c}};
  EXPECT_EQ(stream.str(), "{a} => {b, c}");
  stream.str("");

  stream << FunctionalDependency{{_a, _b}, {_c}};
  EXPECT_EQ(stream.str(), "{a, b} => {c}");
}

TEST_F(FunctionalDependencyTest, InflateFDsSimple) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_1 = FunctionalDependency({_a}, {_b});
  const auto fd_a_2 = FunctionalDependency({_a}, {_c});
  const auto fd_a_b = FunctionalDependency({_a, _b}, {_c});
  const auto fd_x = FunctionalDependency({_x}, {_y});

  const auto& inflated_fds = inflate_fds({fd_a, fd_a_b, fd_x, fd_x});
  EXPECT_EQ(inflated_fds.size(), 4);
  EXPECT_FALSE(inflated_fds.contains(fd_a));
  EXPECT_TRUE(inflated_fds.contains(fd_a_1));
  EXPECT_TRUE(inflated_fds.contains(fd_a_2));
  EXPECT_TRUE(inflated_fds.contains(fd_a_b));
  EXPECT_TRUE(inflated_fds.contains(fd_x));
}

TEST_F(FunctionalDependencyTest, InflateFDsGenuineAndSpurious) {
  const auto fd_a_bc = FunctionalDependency({_a}, {_b, _c}, /*is_genuine=*/true);
  const auto fd_a_bc2 = FunctionalDependency({_a}, {_b, _c, _x}, /*is_genuine=*/false);
  const auto fd_a_b = FunctionalDependency({_a}, {_b}, /*is_genuine=*/true);
  const auto fd_a_c = FunctionalDependency({_a}, {_c}, /*is_genuine=*/true);
  const auto fd_a_x = FunctionalDependency({_a}, {_x}, /*is_genuine=*/false);

  const auto fd_b_ac = FunctionalDependency({_b}, {_a, _c}, /*is_genuine=*/false);
  const auto fd_b_a = FunctionalDependency({_b}, {_a}, /*is_genuine=*/false);
  const auto fd_b_c = FunctionalDependency({_b}, {_c}, /*is_genuine=*/false);

  const auto& inflated_fds = inflate_fds({fd_a_bc, fd_a_bc2, fd_b_ac});
  EXPECT_EQ(inflated_fds.size(), 5);
  ASSERT_TRUE(inflated_fds.contains(fd_a_b));
  ASSERT_TRUE(inflated_fds.contains(fd_a_c));
  ASSERT_TRUE(inflated_fds.contains(fd_a_x));
  ASSERT_TRUE(inflated_fds.contains(fd_b_c));
  ASSERT_TRUE(inflated_fds.contains(fd_b_a));

  // Also check that the genuine properties are correct. Note that FDs are not hashed by their genuine
  // properties, so we cannot check the genuine property of the inflated FDs directly
  EXPECT_TRUE(inflated_fds.find(fd_a_b)->is_genuine());
  EXPECT_TRUE(inflated_fds.find(fd_a_c)->is_genuine());
  EXPECT_FALSE(inflated_fds.find(fd_a_x)->is_genuine());
  EXPECT_FALSE(inflated_fds.find(fd_b_a)->is_genuine());
  EXPECT_FALSE(inflated_fds.find(fd_b_c)->is_genuine());

  // An FD with multiple dependents is not inflated.
  EXPECT_FALSE(inflated_fds.contains(fd_b_ac));
}

TEST_F(FunctionalDependencyTest, DeflateFDsSimple) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_1 = FunctionalDependency({_a}, {_b});
  const auto fd_a_2 = FunctionalDependency({_a}, {_c});
  const auto fd_b_c = FunctionalDependency({_b, _c}, {_a});

  const auto& deflated_fds = deflate_fds({fd_a_1, fd_a_2, fd_a_2, fd_b_c});
  EXPECT_EQ(deflated_fds.size(), 2);
  EXPECT_TRUE(deflated_fds.contains(fd_a));
  EXPECT_TRUE(deflated_fds.contains(fd_b_c));
}

TEST_F(FunctionalDependencyTest, DeflateFDsGenuineAndSpurious) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c}, /*is_genuine=*/false);
  const auto fd_a_b = FunctionalDependency({_a}, {_b}, /*is_genuine=*/true);
  const auto fd_a_c = FunctionalDependency({_a}, {_c}, /*is_genuine=*/false);

  const auto& deflated_fds = deflate_fds({fd_a_b, fd_a_c});
  EXPECT_EQ(deflated_fds.size(), 2);
  ASSERT_TRUE(deflated_fds.contains(fd_a));    // We combine the genuine and non-genuine FDs into a non-genuine FD.
  ASSERT_TRUE(deflated_fds.contains(fd_a_b));  // We keep the genuine FD.

  // Check that the genuine property is correct.
  EXPECT_FALSE(deflated_fds.find(fd_a)->is_genuine());
  EXPECT_TRUE(deflated_fds.find(fd_a_b)->is_genuine());
}

TEST_F(FunctionalDependencyTest, UnionFDsEmpty) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});

  EXPECT_TRUE(union_fds({}, {}).empty());
  EXPECT_EQ(union_fds({fd_a}, {}), FunctionalDependencies{fd_a});
  EXPECT_EQ(union_fds({}, {fd_a}), FunctionalDependencies{fd_a});
}

TEST_F(FunctionalDependencyTest, UnionFDs) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_1 = FunctionalDependency({_a}, {_b});
  const auto fd_a_2 = FunctionalDependency({_a}, {_c});
  const auto fd_a_b = FunctionalDependency({_a, _b}, {_c});
  const auto fd_b = FunctionalDependency({_b}, {_c});

  const auto& fds_unified = union_fds({fd_a_1, fd_a_b, fd_b}, {fd_a_2});

  EXPECT_EQ(fds_unified.size(), 3);
  EXPECT_TRUE(fds_unified.contains(fd_a));
  EXPECT_TRUE(fds_unified.contains(fd_b));
  EXPECT_TRUE(fds_unified.contains(fd_a_b));
}

TEST_F(FunctionalDependencyTest, UnionFDsRemoveDuplicates) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_b = FunctionalDependency({_b}, {_c});

  const auto& fds_unified = union_fds({fd_a, fd_b}, {fd_b});

  EXPECT_EQ(fds_unified.size(), 2);
  EXPECT_TRUE(fds_unified.contains(fd_a));
  EXPECT_TRUE(fds_unified.contains(fd_b));
}

TEST_F(FunctionalDependencyTest, IntersectFDsEmpty) {
  const auto fd_x = FunctionalDependency({_x}, {_y});

  EXPECT_TRUE(intersect_fds({}, {}).empty());
  EXPECT_TRUE(intersect_fds({fd_x}, {}).empty());
  EXPECT_TRUE(intersect_fds({}, {fd_x}).empty());
}

TEST_F(FunctionalDependencyTest, IntersectFDs) {
  const auto fd_a = FunctionalDependency({_a}, {_b, _c});
  const auto fd_a_1 = FunctionalDependency({_a}, {_b});
  const auto fd_a_2 = FunctionalDependency({_a}, {_c});
  const auto fd_a_b = FunctionalDependency({_a, _b}, {_c});
  const auto fd_x = FunctionalDependency({_x}, {_y});

  const auto& intersected_fds = intersect_fds({fd_a, fd_a_b, fd_x}, {fd_a_b, fd_a_2});
  EXPECT_EQ(intersected_fds.size(), 2);
  EXPECT_TRUE(intersected_fds.contains(fd_a_b));
  EXPECT_TRUE(intersected_fds.contains(fd_a_2));
}

}  // namespace hyrise
