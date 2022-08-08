#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <ostream>

namespace opossum {

class Table;

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
                                       size_t& id_counter);

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
                                  const NodePrintFn<Node>& print_node_fn, std::ostream& stream = std::cout);

/**
 * Utility for formatted printing of table key constraints.
 *
 * Results look comparable to this
 *
 * PRIMARY_KEY(a), UNIQUE(b, c)
 *
 * @param table             Table the key constraints belong to
 * @param stream            The stream to print on
 * @param separator         String that is printed between different table key constraints
 */
void print_table_key_constraints(const std::shared_ptr<const Table>& table, std::ostream& stream,
                                 const std::string& separator = ", ");

}  //  namespace opossum
