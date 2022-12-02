#include "base_test.hpp"

#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace hyrise {

class OrderDependencyTest : public BaseTest {
 public:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "mock_node_a");
    _a_a = _mock_node_a->get_column("a");
    _a_b = _mock_node_a->get_column("b");
    _a_c = _mock_node_a->get_column("c");

    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "mock_node_b");
    _b_a = _mock_node_b->get_column("a");
    _b_b = _mock_node_b->get_column("b");
  }

 protected:
  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a_a, _a_b, _a_c, _b_a, _b_b;
};

TEST_F(OrderDependencyTest, InvalidDependencies) {
  EXPECT_THROW(OrderDependency({}, {_a_a}), std::logic_error);
  EXPECT_THROW(OrderDependency({_a_a}, {}), std::logic_error);
}

TEST_F(OrderDependencyTest, Equals) {
  const auto od_a_to_b = OrderDependency({_a_a}, {_a_b});
  const auto od_a_to_b_c = OrderDependency({_a_a}, {_a_b, _a_c});
  const auto od_a_b_to_c = OrderDependency({_a_a, _a_b}, {_a_c});

  // Equal
  EXPECT_EQ(od_a_to_b, OrderDependency({_a_a}, {_a_b}));
  EXPECT_EQ(od_a_to_b_c, OrderDependency({_a_a}, {_a_b, _a_c}));
  EXPECT_EQ(od_a_b_to_c, OrderDependency({_a_a, _a_b}, {_a_c}));

  // Not Equal
  EXPECT_NE(od_a_to_b, OrderDependency({_a_b}, {_a_a}));
  EXPECT_NE(od_a_to_b, OrderDependency({_a_c}, {_a_b}));
  EXPECT_NE(od_a_to_b, OrderDependency({_a_a}, {_a_c}));
  EXPECT_NE(od_a_to_b, OrderDependency({_b_a}, {_b_b}));
  EXPECT_NE(od_a_to_b, od_a_to_b_c);
  EXPECT_NE(od_a_to_b, od_a_b_to_c);
  EXPECT_NE(od_a_to_b_c, OrderDependency({_a_a}, {_a_c, _a_b}));
  EXPECT_NE(od_a_to_b_c, od_a_b_to_c);
  EXPECT_NE(od_a_b_to_c, OrderDependency({_a_b, _a_a}, {_a_c}));
}

TEST_F(OrderDependencyTest, Hash) {
  const auto od_a_to_b = OrderDependency{{_a_a}, {_a_b}};
  const auto od_a_to_b_c = OrderDependency{{_a_a}, {_a_b, _a_c}};
  const auto od_a_b_to_c = OrderDependency{{_a_a, _a_b}, {_a_c}};

  // Equal Hash
  EXPECT_EQ(od_a_to_b.hash(), OrderDependency({_a_a}, {_a_b}).hash());
  EXPECT_EQ(od_a_to_b_c.hash(), OrderDependency({_a_a}, {_a_b, _a_c}).hash());
  EXPECT_EQ(od_a_b_to_c.hash(), OrderDependency({_a_a, _a_b}, {_a_c}).hash());

  // Not Equal Hash
  EXPECT_NE(od_a_to_b.hash(), OrderDependency({_a_b}, {_a_a}).hash());
  EXPECT_NE(od_a_to_b.hash(), OrderDependency({_a_c}, {_a_b}).hash());
  EXPECT_NE(od_a_to_b.hash(), OrderDependency({_a_a}, {_a_c}).hash());
  EXPECT_NE(od_a_to_b.hash(), OrderDependency({_b_a}, {_b_b}).hash());
  EXPECT_NE(od_a_to_b.hash(), od_a_to_b_c.hash());
  EXPECT_NE(od_a_to_b.hash(), od_a_b_to_c.hash());
  EXPECT_NE(od_a_to_b_c.hash(), OrderDependency({_a_a}, {_a_c, _a_b}).hash());
  EXPECT_NE(od_a_to_b_c.hash(), od_a_b_to_c.hash());
  EXPECT_NE(od_a_b_to_c.hash(), OrderDependency({_a_b, _a_a}, {_a_c}).hash());
}

TEST_F(OrderDependencyTest, Container) {
  const auto od_a_to_b = OrderDependency{{_a_a}, {_a_b}};
  const auto od_a_to_b_c = OrderDependency{{_a_a}, {_a_b, _a_c}};
  const auto od_a_b_to_c = OrderDependency{{_a_a, _a_b}, {_a_c}};
  const auto od_b_a_to_b_b = OrderDependency{{_b_a}, {_b_b}};

  auto order_dependencies = OrderDependencies{};
  EXPECT_TRUE(order_dependencies.empty());

  order_dependencies.emplace(od_a_to_b);
  EXPECT_EQ(order_dependencies.size(), 1);
  EXPECT_TRUE(order_dependencies.contains(od_a_to_b));

  order_dependencies.emplace(od_a_to_b);
  EXPECT_EQ(order_dependencies.size(), 1);

  order_dependencies.emplace(od_a_to_b_c);
  EXPECT_EQ(order_dependencies.size(), 2);
  EXPECT_TRUE(order_dependencies.contains(od_a_to_b_c));

  order_dependencies.emplace(od_a_b_to_c);
  EXPECT_EQ(order_dependencies.size(), 3);
  EXPECT_TRUE(order_dependencies.contains(od_a_b_to_c));

  order_dependencies.emplace(od_b_a_to_b_b);
  EXPECT_EQ(order_dependencies.size(), 4);
  EXPECT_TRUE(order_dependencies.contains(od_b_a_to_b_b));
}

}  // namespace hyrise
