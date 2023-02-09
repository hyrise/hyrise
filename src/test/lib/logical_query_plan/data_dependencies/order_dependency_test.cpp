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
    _b_x = _mock_node_b->get_column("a");
    _b_y = _mock_node_b->get_column("b");
  }

 protected:
  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a_a, _a_b, _a_c, _b_x, _b_y;
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
  EXPECT_NE(od_a_to_b, OrderDependency({_b_x}, {_b_y}));
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
  EXPECT_NE(od_a_to_b.hash(), OrderDependency({_b_x}, {_b_y}).hash());
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
  const auto od_x_to_y = OrderDependency{{_b_x}, {_b_y}};

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

  order_dependencies.emplace(od_x_to_y);
  EXPECT_EQ(order_dependencies.size(), 4);
  EXPECT_TRUE(order_dependencies.contains(od_x_to_y));
}

TEST_F(OrderDependencyTest, BuildTransitiveODClosure) {
  const auto od_a_to_b = OrderDependency({_a_a}, {_a_b});
  const auto od_x_to_y = OrderDependency({_b_x}, {_b_y});

  // Do nothing if ODs are not transitive.
  auto order_dependencies = OrderDependencies{od_a_to_b, od_x_to_y};
  build_transitive_od_closure(order_dependencies);
  EXPECT_EQ(order_dependencies.size(), 2);
  EXPECT_TRUE(order_dependencies.contains(od_a_to_b));
  EXPECT_TRUE(order_dependencies.contains(od_x_to_y));

  // Build transitive ODs.
  const auto od_b_to_x = OrderDependency({_a_b}, {_b_x});
  order_dependencies.emplace(od_b_to_x);
  EXPECT_EQ(order_dependencies.size(), 3);
  EXPECT_TRUE(order_dependencies.contains(od_b_to_x));

  build_transitive_od_closure(order_dependencies);
  EXPECT_EQ(order_dependencies.size(), 6);
  const auto od_a_to_x = OrderDependency({_a_a}, {_b_x});
  EXPECT_TRUE(order_dependencies.contains(od_a_to_x));
  const auto od_a_to_y = OrderDependency({_a_a}, {_b_y});
  EXPECT_TRUE(order_dependencies.contains(od_a_to_y));
  const auto od_b_to_y = OrderDependency({_a_b}, {_b_y});
  EXPECT_TRUE(order_dependencies.contains(od_b_to_y));

  // Terminate and do not add ODs with the same expression on both sides if there are circles.
  const auto od_y_to_a = OrderDependency({_b_y}, {_a_a});
  order_dependencies.emplace(od_y_to_a);
  EXPECT_EQ(order_dependencies.size(), 7);
  EXPECT_TRUE(order_dependencies.contains(od_y_to_a));

  build_transitive_od_closure(order_dependencies);
  EXPECT_EQ(order_dependencies.size(), 12);
  EXPECT_TRUE(order_dependencies.contains(od_y_to_a));
  const auto od_b_to_a = OrderDependency({_a_b}, {_a_a});
  EXPECT_TRUE(order_dependencies.contains(od_b_to_a));
  const auto od_x_to_a = OrderDependency({_b_x}, {_a_a});
  EXPECT_TRUE(order_dependencies.contains(od_x_to_a));
  const auto od_x_to_b = OrderDependency({_b_x}, {_a_b});
  EXPECT_TRUE(order_dependencies.contains(od_x_to_b));
  const auto od_y_to_b = OrderDependency({_b_y}, {_a_b});
  EXPECT_TRUE(order_dependencies.contains(od_y_to_b));
  const auto od_y_to_x = OrderDependency({_b_y}, {_b_x});
  EXPECT_TRUE(order_dependencies.contains(od_y_to_x));
}

}  // namespace hyrise
