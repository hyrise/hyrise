#include "base_test.hpp"

#include "logical_query_plan/data_dependencies/inclusion_dependency.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace hyrise {

class InclusionDependencyTest : public BaseTest {
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
  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _x, _y;
};

TEST_F(InclusionDependencyTest, InvalidDependencies) {
  EXPECT_THROW(InclusionDependency({}, {_a}), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a}, {}), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a}, {_x, _y}), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a, _b}, {_x}), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a, _b}, {_x, _c}), std::logic_error);
  EXPECT_NO_THROW(InclusionDependency({_a}, {_b}));
}

TEST_F(InclusionDependencyTest, Equals) {
  const auto ind_a_x = InclusionDependency{{_a}, {_x}};
  const auto ind_a_b_x_y = InclusionDependency{{_a, _b}, {_x, _y}};

  // Equal.
  EXPECT_EQ(ind_a_x, InclusionDependency({_a}, {_x}));
  EXPECT_EQ(ind_a_b_x_y, InclusionDependency({_a, _b}, {_x, _y}));

  // Not equal. INDs with swapped columns should have been covered by the corresponding table constraints.
  EXPECT_NE(ind_a_x, InclusionDependency({_a}, {_y}));
  EXPECT_NE(ind_a_x, InclusionDependency({_b}, {_x}));
  EXPECT_NE(ind_a_x, InclusionDependency({_x}, {_a}));
  EXPECT_NE(ind_a_x, ind_a_b_x_y);
  EXPECT_NE(ind_a_b_x_y, InclusionDependency({_a, _b}, {_y, _x}));
  EXPECT_NE(ind_a_b_x_y, InclusionDependency({_b, _a}, {_x, _y}));
  EXPECT_NE(ind_a_b_x_y, InclusionDependency({_b, _a}, {_y, _x}));
}

TEST_F(InclusionDependencyTest, Hash) {
  const auto ind_a_x = InclusionDependency{{_a}, {_x}};
  const auto ind_a_b_x_y = InclusionDependency{{_a, _b}, {_x, _y}};

  // Equal hash.
  EXPECT_EQ(ind_a_x.hash(), InclusionDependency({_a}, {_x}).hash());
  EXPECT_EQ(ind_a_b_x_y.hash(), InclusionDependency({_a, _b}, {_x, _y}).hash());

  // Not equal hash. INDs with swapped columns should have been covered by the corresponding table constraints.
  EXPECT_NE(ind_a_x.hash(), InclusionDependency({_a}, {_y}).hash());
  EXPECT_NE(ind_a_x.hash(), InclusionDependency({_b}, {_x}).hash());
  EXPECT_NE(ind_a_x.hash(), InclusionDependency({_x}, {_a}).hash());
  EXPECT_NE(ind_a_x.hash(), ind_a_b_x_y.hash());
  EXPECT_NE(ind_a_b_x_y.hash(), InclusionDependency({_a, _b}, {_y, _x}).hash());
  EXPECT_NE(ind_a_b_x_y.hash(), InclusionDependency({_b, _a}, {_x, _y}).hash());
  EXPECT_NE(ind_a_b_x_y.hash(), InclusionDependency({_b, _a}, {_y, _x}).hash());
}

TEST_F(InclusionDependencyTest, Container) {
  const auto ind_a_x = InclusionDependency{{_a}, {_x}};
  const auto ind_x_a = InclusionDependency{{_x}, {_a}};
  const auto ind_a_b_x_y = InclusionDependency{{_a, _b}, {_x, _y}};
  const auto ind_b_a_x_y = InclusionDependency{{_b, _a}, {_x, _y}};
  const auto ind_a_b_y_x = InclusionDependency{{_a, _b}, {_y, _x}};
  const auto ind_b_a_y_x = InclusionDependency{{_b, _a}, {_y, _x}};

  auto inclusion_dependencies = InclusionDependencies{};
  EXPECT_TRUE(inclusion_dependencies.empty());

  inclusion_dependencies.emplace(ind_a_x);
  EXPECT_EQ(inclusion_dependencies.size(), 1);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_x));

  inclusion_dependencies.emplace(ind_a_x);
  EXPECT_EQ(inclusion_dependencies.size(), 1);

  inclusion_dependencies.emplace(ind_x_a);
  EXPECT_EQ(inclusion_dependencies.size(), 2);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_x_a));

  inclusion_dependencies.emplace(ind_a_b_x_y);
  EXPECT_EQ(inclusion_dependencies.size(), 3);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_b_x_y));

  inclusion_dependencies.emplace(ind_b_a_x_y);
  EXPECT_EQ(inclusion_dependencies.size(), 4);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b_a_x_y));

  inclusion_dependencies.emplace(ind_a_b_y_x);
  EXPECT_EQ(inclusion_dependencies.size(), 5);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_b_y_x));

  inclusion_dependencies.emplace(ind_b_a_y_x);
  EXPECT_EQ(inclusion_dependencies.size(), 6);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b_a_y_x));
}

}  // namespace hyrise
