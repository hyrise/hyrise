#include "base_test.hpp"

#include "logical_query_plan/data_dependencies/inclusion_dependency.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace hyrise {

class InclusionDependencyTest : public BaseTest {
 public:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "mock_node_a");
    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
    _c = _mock_node->get_column("c");

    _table_a = Table::create_dummy_table({{"a", DataType::Int, false}});
    _table_b = Table::create_dummy_table({{"a", DataType::Int, false}});
  }

 protected:
  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<Table> _table_a, _table_b;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c;
};

TEST_F(InclusionDependencyTest, InvalidDependencies) {
  EXPECT_THROW(InclusionDependency({}, {ColumnID{0}}, _table_a), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a}, {}, _table_a), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a}, {ColumnID{0}, ColumnID{1}}, _table_a), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a, _b}, {ColumnID{0}}, _table_a), std::logic_error);
  EXPECT_THROW(InclusionDependency({_a}, {ColumnID{0}}, nullptr), std::logic_error);
  EXPECT_NO_THROW(InclusionDependency({_a}, {ColumnID{0}}, _table_a));
}

TEST_F(InclusionDependencyTest, Equals) {
  const auto ind_a_0 = InclusionDependency{{_a}, {ColumnID{0}}, _table_a};
  const auto ind_a_b_0_1 = InclusionDependency{{_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_a};

  // Equal.
  EXPECT_EQ(ind_a_0, InclusionDependency({_a}, {ColumnID{0}}, _table_a));
  EXPECT_EQ(ind_a_b_0_1, InclusionDependency({_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_a));

  // Not equal. INDs with swapped columns should have been covered by the corresponding table constraints.
  EXPECT_NE(ind_a_0, InclusionDependency({_a}, {ColumnID{1}}, _table_a));
  EXPECT_NE(ind_a_0, InclusionDependency({_b}, {ColumnID{0}}, _table_a));
  EXPECT_NE(ind_a_0, InclusionDependency({_a}, {ColumnID{0}}, _table_b));
  EXPECT_NE(ind_a_0, ind_a_b_0_1);
  EXPECT_NE(ind_a_b_0_1, InclusionDependency({_a, _b}, {ColumnID{1}, ColumnID{0}}, _table_a));
  EXPECT_NE(ind_a_b_0_1, InclusionDependency({_b, _a}, {ColumnID{0}, ColumnID{1}}, _table_a));
  EXPECT_NE(ind_a_b_0_1, InclusionDependency({_b, _a}, {ColumnID{0}, ColumnID{1}}, _table_b));
}

TEST_F(InclusionDependencyTest, Hash) {
  const auto ind_a_0 = InclusionDependency{{_a}, {ColumnID{0}}, _table_a};
  const auto ind_a_b_0_1 = InclusionDependency{{_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_a};

  // Equal hash.
  EXPECT_EQ(ind_a_0.hash(), InclusionDependency({_a}, {ColumnID{0}}, _table_a).hash());
  EXPECT_EQ(ind_a_b_0_1.hash(), InclusionDependency({_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_a).hash());

  // Not equal hash. INDs with swapped columns should have been covered by the corresponding table constraints.
  EXPECT_NE(ind_a_0.hash(), InclusionDependency({_a}, {ColumnID{1}}, _table_a).hash());
  EXPECT_NE(ind_a_0.hash(), InclusionDependency({_b}, {ColumnID{0}}, _table_a).hash());
  EXPECT_NE(ind_a_0.hash(), InclusionDependency({_a}, {ColumnID{0}}, _table_b).hash());
  EXPECT_NE(ind_a_0.hash(), ind_a_b_0_1.hash());
  EXPECT_NE(ind_a_b_0_1.hash(), InclusionDependency({_a, _b}, {ColumnID{1}, ColumnID{0}}, _table_a).hash());
  EXPECT_NE(ind_a_b_0_1.hash(), InclusionDependency({_b, _a}, {ColumnID{0}, ColumnID{1}}, _table_a).hash());
  EXPECT_NE(ind_a_b_0_1.hash(), InclusionDependency({_b, _a}, {ColumnID{0}, ColumnID{1}}, _table_b).hash());
}

TEST_F(InclusionDependencyTest, Container) {
  const auto ind_a_0 = InclusionDependency{{_a}, {ColumnID{0}}, _table_a};
  const auto ind_a_1 = InclusionDependency{{_a}, {ColumnID{1}}, _table_a};
  const auto ind_a_0_b = InclusionDependency{{_a}, {ColumnID{0}}, _table_b};
  const auto ind_b_0 = InclusionDependency{{_b}, {ColumnID{0}}, _table_a};
  const auto ind_a_b_0_1 = InclusionDependency{{_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_a};
  const auto ind_b_a_0_1 = InclusionDependency{{_b, _a}, {ColumnID{0}, ColumnID{1}}, _table_a};
  const auto ind_a_b_1_0 = InclusionDependency{{_a, _b}, {ColumnID{1}, ColumnID{0}}, _table_a};
  const auto ind_b_a_1_0 = InclusionDependency{{_b, _a}, {ColumnID{1}, ColumnID{0}}, _table_a};
  const auto ind_a_b_0_1_b = InclusionDependency{{_a, _b}, {ColumnID{0}, ColumnID{1}}, _table_b};

  auto inclusion_dependencies = InclusionDependencies{};
  EXPECT_TRUE(inclusion_dependencies.empty());

  inclusion_dependencies.emplace(ind_a_0);
  EXPECT_EQ(inclusion_dependencies.size(), 1);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_0));

  inclusion_dependencies.emplace(ind_a_0);
  EXPECT_EQ(inclusion_dependencies.size(), 1);

  inclusion_dependencies.emplace(ind_a_1);
  EXPECT_EQ(inclusion_dependencies.size(), 2);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_1));

  inclusion_dependencies.emplace(ind_a_0_b);
  EXPECT_EQ(inclusion_dependencies.size(), 3);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_0_b));

  inclusion_dependencies.emplace(ind_b_0);
  EXPECT_EQ(inclusion_dependencies.size(), 4);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b_0));

  inclusion_dependencies.emplace(ind_a_b_0_1);
  EXPECT_EQ(inclusion_dependencies.size(), 5);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_b_0_1));

  inclusion_dependencies.emplace(ind_b_a_0_1);
  EXPECT_EQ(inclusion_dependencies.size(), 6);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b_a_0_1));

  inclusion_dependencies.emplace(ind_a_b_1_0);
  EXPECT_EQ(inclusion_dependencies.size(), 7);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_b_1_0));

  inclusion_dependencies.emplace(ind_b_a_1_0);
  EXPECT_EQ(inclusion_dependencies.size(), 8);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b_a_1_0));

  inclusion_dependencies.emplace(ind_a_b_0_1_b);
  EXPECT_EQ(inclusion_dependencies.size(), 9);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a_b_0_1_b));
}

}  // namespace hyrise
