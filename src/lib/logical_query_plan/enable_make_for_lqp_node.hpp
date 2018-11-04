#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace opossum {

class AbstractLQPNode;

/**
 * LQP node types should derive from this in order to enable the <NodeType>::make() function that allows for a clean
 * notation when building LQPs via code by allowing to pass in a nodes input(ren) as the last argument(s).
 *
 * const auto input_lqp =
 * PredicateNode::make(_mock_node_a, PredicateCondition::Equals, 42,
 *   PredicateNode::make(_mock_node_b, PredicateCondition::GreaterThan, 50,
 *     PredicateNode::make(_mock_node_b, PredicateCondition::GreaterThan, 40,
 *       ProjectionNode::make_pass_through(
 *         PredicateNode::make(_mock_node_a, PredicateCondition::GreaterThanEquals, 90,
 *           PredicateNode::make(_mock_node_c, PredicateCondition::LessThan, 500,
 *             _mock_node))))));
 */
template <typename DerivedNode>
class EnableMakeForLQPNode {
 public:
  template <int N, typename... Ts>
  using NthTypeOf = std::tuple_element_t<N, std::tuple<Ts...>>;

  template <typename... Args>
  static std::shared_ptr<DerivedNode> make(Args&&... args) {
    // clang-format off

    // - using nesting instead of && because both sides of the && would need to be valid
    // - redundant else paths instead of one fallthrough at the end, because it too, needs to be valid.
    if constexpr (sizeof...(Args) > 0) {
      if constexpr (std::is_convertible_v<NthTypeOf<sizeof...(Args)-1, Args...>, std::shared_ptr<AbstractLQPNode>>) {
        auto args_tuple = std::forward_as_tuple(args...);
        if constexpr (sizeof...(Args) > 1) {
          if constexpr (std::is_convertible_v<NthTypeOf<sizeof...(Args)-2, Args...>, std::shared_ptr<AbstractLQPNode>>) {  // NOLINT - too long, but better than breaking
            // last two arguments are shared_ptr<AbstractLQPNode>
            auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args) - 2>());
            node->set_left_input(std::get<sizeof...(Args) - 2>(args_tuple));
            node->set_right_input(std::get<sizeof...(Args) - 1>(args_tuple));
            return node;
          } else {
            // last argument is shared_ptr<AbstractLQPNode>
            auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args)-1>());
            node->set_left_input(std::get<sizeof...(Args)-1>(args_tuple));
            return node;
          }
        } else {
          // last argument is shared_ptr<AbstractLQPNode>
          auto node = make_impl(args_tuple, std::make_index_sequence<sizeof...(Args)-1>());
          node->set_left_input(std::get<sizeof...(Args)-1>(args_tuple));
          return node;
        }
      } else {
        // no shared_ptr<AbstractLQPNode> was passed at the end
        return make_impl(std::forward_as_tuple(args...), std::make_index_sequence<sizeof...(Args)-0>());
      }
    } else {
      // no shared_ptr<AbstractLQPNode> was passed at the end
      return make_impl(std::forward_as_tuple(args...), std::make_index_sequence<sizeof...(Args)-0>());
    }
    // clang-format on
  }

 private:
  template <class Tuple, size_t... I>
  static std::shared_ptr<DerivedNode> make_impl(const Tuple& constructor_arguments,
                                                std::index_sequence<I...> num_constructor_args) {
    return std::make_shared<DerivedNode>(std::get<I>(constructor_arguments)...);
  }
};
}  // namespace opossum
