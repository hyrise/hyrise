#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hyrise {

class AbstractLQPNode;

/**
 * With this class, LQP nodes inherit a static <DerivedNode>::make construction function that allows for convenience and
 * a clean notation in tests. The ::make function creates an LQP node by passing the first few function arguments to the
 * appropriate LQP node constructor. Furthermore, the left and right inputs of the generated LQP node are set from the
 * last (two) function argument(s), if provided.
 *
 * The usage of ::make is intended as follows:
 *  auto lqp =
 *  JoinNode::make(JoinMode::Semi, equals_(b_y, literal),
 *    JoinNode::make(JoinMode::Inner, equals_(a_a, b_x),
 *      UnionNode::Make(SetOperationMode::All,
 *        PredicateNode::make(greater_than_(a_a, 700),
 *          node_a),
 *        PredicateNode::make(less_than_(a_b, 123),
 *          node_a)),
 *      node_b),
 *    ProjectionNode::make(expression_vector(literal),
 *      dummy_table_node));
 */
template <typename DerivedNode>
class EnableMakeForLQPNode {
 public:
  // The following declaration is a variadic function template taking any number of arguments of arbitrary types.
  template <typename... ArgumentTypes>
  static std::shared_ptr<DerivedNode> make(ArgumentTypes&&... arguments) {
    if constexpr (sizeof...(ArgumentTypes) > 0) {
      auto arguments_tuple = std::forward_as_tuple(arguments...);

      // Check if the last function argument represents an LQP node (that can be set as an input).
      if constexpr (IsLQPNodeArgument<sizeof...(ArgumentTypes) - 1, ArgumentTypes...>::value) {
        if constexpr (sizeof...(ArgumentTypes) > 1) {
          // Check if the second to last function argument represents an LQP node as well.
          if constexpr (IsLQPNodeArgument<sizeof...(ArgumentTypes) - 2, ArgumentTypes...>::value) {
            // Use function arguments, except for the last two, to construct the LQP node.
            auto node = create_lqp_node(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes) - 2>());
            node->set_left_input(std::get<sizeof...(ArgumentTypes) - 2>(arguments_tuple));
            node->set_right_input(std::get<sizeof...(ArgumentTypes) - 1>(arguments_tuple));
            return node;
          } else {
            // Use function arguments, except for the last, to construct the LQP node.
            auto node = create_lqp_node(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes) - 1>());
            node->set_left_input(std::get<sizeof...(ArgumentTypes) - 1>(arguments_tuple));
            return node;
          }
        } else {
          // Only one LQP input node was provided as an argument.
          auto node = std::make_shared<DerivedNode>();
          node->set_left_input(std::get<0>(arguments_tuple));
          return node;
        }
      } else {
        // Additional LQP input nodes were not passed as last arguments.
        return create_lqp_node(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes)>());
      }
    } else {
      // No arguments were passed to this function.
      return std::make_shared<DerivedNode>();
    }
  }

 private:
  template <class ArgumentsTupleType, size_t... ConstructorIndices>
  static std::shared_ptr<DerivedNode> create_lqp_node(const ArgumentsTupleType& arguments_tuple,
                                                      std::index_sequence<ConstructorIndices...> /* indices */) {
    return std::make_shared<DerivedNode>(std::get<ConstructorIndices>(arguments_tuple)...);
  }

  template <size_t ArgumentIndex, typename... ArgumentTypes>
  using IsLQPNodeArgument = std::is_convertible<std::tuple_element_t<ArgumentIndex, std::tuple<ArgumentTypes...>>,
                                                std::shared_ptr<AbstractLQPNode>>;
};

}  // namespace hyrise
