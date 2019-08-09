#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <ostream>

namespace opossum {

template <typename Node>
using NodeGetChildrenFn = std::function<std::vector<std::shared_ptr<Node>>(const std::shared_ptr<Node>&)>;

template <typename Node>
using NodePrintFn = std::function<void(const std::shared_ptr<Node>&, std::ostream& stream)>;

namespace detail {

/**
 *
 * @param indentation   its size determines the indentation of a node, a true means a vertical line "|",
 *                      a false means, a space " " should be used to increase the indentation
 * @param id_by_node    used to determine whether a node was already printed and which id it had
 * @param id_counter    used to generate ids for nodes
 */
template <typename Node>
void print_directed_acyclic_graph_impl(const std::shared_ptr<Node>& node,
                                       const NodeGetChildrenFn<Node>& get_children_fn,
                                       const NodePrintFn<Node>& node_print_fn, std::ostream& stream,
                                       std::vector<bool>& indentation,
                                       std::unordered_map<std::shared_ptr<const Node>, size_t>& id_by_node,
                                       size_t& id_counter) {
  /**
   * Indent whilst drawing the edges
   */
  const auto max_indentation = indentation.empty() ? 0 : indentation.size() - 1;
  for (size_t level = 0; level < max_indentation; ++level) {
    if (indentation[level]) {
      stream << " | ";
    } else {
      stream << "   ";
    }
  }

  // Only the root node is not "pointed at" with "\_<node_info>"
  if (!indentation.empty()) {
    stream << " \\_";
  }

  /**
   * Check whether the node has been printed before
   */
  const auto iter = id_by_node.find(node);
  if (iter != id_by_node.end()) {
    stream << "Recurring Node --> [" << iter->second << "]" << std::endl;
    return;
  }

  const auto this_node_id = id_counter;
  id_counter++;
  id_by_node.emplace(node, this_node_id);

  /**
   * Print node info
   */
  stream << "[" << this_node_id << "] ";
  node_print_fn(node, stream);
  stream << std::endl;

  const auto children = get_children_fn(node);
  indentation.emplace_back(true);

  /**
   * Recursively progress to children
   */
  for (size_t child_idx = 0; child_idx < children.size(); ++child_idx) {
    if (child_idx + 1 == children.size()) indentation.back() = false;
    print_directed_acyclic_graph_impl<Node>(children[child_idx], get_children_fn, node_print_fn, stream, indentation,
                                            id_by_node, id_counter);
  }

  indentation.pop_back();
}
}  // namespace detail

/**
 * Utility for formatted printing of any Directed Acyclic Graph.
 *
 * Results look comparable to this
 *
 * [0] [Cross Join]
 *  \_[1] [Cross Join]
 *  |  \_[2] [Predicate] a = 42
 *  |  |  \_[3] [Cross Join]
 *  |  |     \_[4] [MockTable]
 *  |  |     \_[5] [MockTable]
 *  |  \_[6] [Cross Join]
 *  |     \_Recurring Node --> [3]
 *  |     \_Recurring Node --> [5]
 *  \_[7] [Cross Join]
 *     \_Recurring Node --> [3]
 *     \_Recurring Node --> [5]
 *
 * @param node              The root node originating from which the graph should be printed
 * @param get_children_fn   Callback that returns a nodes children
 * @param print_node_fn     Callback that prints the information about a node, e.g. "[Predicate] a = 42" in the example
 *                              above
 * @param stream            The stream to print on
 */
template <typename Node>
void print_directed_acyclic_graph(const std::shared_ptr<Node>& node, const NodeGetChildrenFn<Node>& get_children_fn,
                                  const NodePrintFn<Node>& print_node_fn, std::ostream& stream = std::cout) {
  std::vector<bool> levels;
  std::unordered_map<std::shared_ptr<Node>, size_t> id_by_node;
  auto id_counter = size_t{0};

  detail::print_directed_acyclic_graph_impl<Node>(node, get_children_fn, print_node_fn, stream, levels, id_by_node,
                                                  id_counter);
}

}  //  namespace opossum
