#pragma once

#include <array>
#include <vector>

#include "enable_make_for_lqp_node.hpp"
#include "lqp_utils.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class TableStatistics;

enum class LQPNodeType {
  Aggregate,
  Alias,
  CreateView,
  Delete,
  DropView,
  DummyTable,
  Insert,
  Join,
  Limit,
  Predicate,
  Projection,
  Root,
  ShowColumns,
  ShowTables,
  Sort,
  StoredTable,
  Update,
  Union,
  Validate,
  Mock
};

enum class LQPInputSide { Left, Right };

// Describes the output of a Node and which of the output's inputs this Node is
struct LQPOutputRelation {
  std::shared_ptr<AbstractLQPNode> output;
  LQPInputSide input_side{LQPInputSide::Left};
};

class AbstractLQPNode : public std::enable_shared_from_this<AbstractLQPNode>, public Noncopyable {
 public:
  explicit AbstractLQPNode(const LQPNodeType node_type);
  virtual ~AbstractLQPNode();

  /**
   * @return a string describing this node, but nothing about its inputs.
   */
  virtual std::string description() const = 0;

  /**
   * @defgroup Access the outputs/inputs
   *
   * The outputs are implicitly set and removed in set_left_input()/set_right_input()/set_input().
   * Design decision: If you delete a node, you explicitly need to call remove_output() on its input.
   *
   * set_input() is a shorthand for set_left_input() or set_right_input(), useful if the side is a runtime value.
   * @{
   */
  std::shared_ptr<AbstractLQPNode> left_input() const;
  std::shared_ptr<AbstractLQPNode> right_input() const;
  std::shared_ptr<AbstractLQPNode> input(LQPInputSide side) const;
  void set_left_input(const std::shared_ptr<AbstractLQPNode>& left);
  void set_right_input(const std::shared_ptr<AbstractLQPNode>& right);
  void set_input(LQPInputSide side, const std::shared_ptr<AbstractLQPNode>& input);

  size_t input_count() const;

  /**
   * @pre this has @param output as an output
   * @return whether this is the left or right input in the specified output.
   */
  LQPInputSide get_input_side(const std::shared_ptr<AbstractLQPNode>& output) const;

  /**
   * @returns {get_output_side(outputs()[0], ..., get_output_side(outputs()[n-1])}
   */
  std::vector<LQPInputSide> get_input_sides() const;

  /**
   * Locks all outputs (as they are stored in weak_ptrs) and returns them as shared_ptrs
   */
  std::vector<std::shared_ptr<AbstractLQPNode>> outputs() const;

  void remove_output(const std::shared_ptr<AbstractLQPNode>& output);
  void clear_outputs();

  /**
   * @return {{outputs()[0], get_input_sides()[0]}, ..., {outputs()[n-1], get_input_sides()[n-1]}}
   */
  std::vector<LQPOutputRelation> output_relations() const;

  /**
   * Same as outputs().size(), but avoids locking all output pointers
   */
  size_t output_count() const;
  /** @} */

  /**
   * @param input_node_mapping     if the LQP contains external expressions, a mapping for the nodes used by them needs
   *                               to be provided
   * @return                       A deep copy of the LQP this Node is the root of
   */
  std::shared_ptr<AbstractLQPNode> deep_copy(LQPNodeMapping input_node_mapping = {}) const;

  /**
   * Compare this node with another, without comparing inputs.
   * @param node_mapping    Mapping from nodes in this node's input plans to corresponding nodes in the input plans of
   *                        rhs
   */
  bool shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const;

  /**
   * @return The Expressions defining each column that this node outputs
   */
  virtual const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const;

  /**
   * @return    All expressions that this node USES (and doesn't just forward)
   *            (e.g., predicates, projections, ...).
   *            Intended, e.g., for the optimizer to have ONE function to call recursively on a plan and see ALL
   *            expressions used in that plan
   */
  virtual std::vector<std::shared_ptr<AbstractExpression>> node_expressions() const;

  /**
   * @return The ColumnID of the @param expression, or std::nullopt if it can't be found
   */
  std::optional<ColumnID> find_column_id(const AbstractExpression& expression) const;

  /**
   * @return The ColumnID of the @param expression. Assert()s that it can be found
   */
  ColumnID get_column_id(const AbstractExpression& expression) const;

  // @{
  /**
   * These functions provide access to statistics for this particular node.
   *
   * AbstractLQPNode::derive_statistics_from() calculates new statistics for this node as they would appear if
   * left_input and right_input WERE its inputs. This works for the actual inputs of this node during the lazy
   * initialization in get_statistics() as well as e.g. in an optimizer rule
   * that tries to reorder nodes based on some statistics. In that case it will call this function for all the nodes
   * that shall be reordered with the same reference node.
   *
   * Inheriting nodes are free to override AbstractLQPNode::derive_statistics_from().
   */
  const std::shared_ptr<TableStatistics> get_statistics();
  virtual std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const;
  // @}

  /**
   * Prints this node and all its descendants (including all Subqueries) formatted as a tree
   */
  void print(std::ostream& out = std::cout) const;

  /**
   * Perform a deep equality check
   */
  bool operator==(const AbstractLQPNode& rhs) const;
  bool operator!=(const AbstractLQPNode& rhs) const;

  const LQPNodeType type;

 protected:
  void _print_impl(std::ostream& out) const;
  virtual std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const = 0;
  virtual bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const = 0;

 private:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(LQPNodeMapping& node_mapping) const;
  std::shared_ptr<AbstractLQPNode> _shallow_copy(LQPNodeMapping& node_mapping) const;

  /**
   * @{
   * For internal usage in set_left_input(), set_right_input(), set_input(), remove_output()
   * Add or remove a output without manipulating this output's input ptr.
   */
  void _add_output_pointer(const std::shared_ptr<AbstractLQPNode>& output);
  void _remove_output_pointer(const AbstractLQPNode& output);
  /** @} */

  std::vector<std::weak_ptr<AbstractLQPNode>> _outputs;
  std::array<std::shared_ptr<AbstractLQPNode>, 2> _inputs;
  std::shared_ptr<TableStatistics> _statistics;
};

}  // namespace opossum
