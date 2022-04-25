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
          // Only one input LQP node was provided as an argument.
          auto node = std::make_shared<DerivedNode>();
          node->set_left_input(std::get<0>(arguments_tuple));
          return node;
        }
      } else {
        // Additional input LQP nodes were not passed as last arguments.
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

}  // namespace opossum
