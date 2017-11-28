#pragma once

#include <memory>
#include <vector>

#include "all_parameter_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

class TransactionContext;

// The SQLQueryPlan holds the operator trees that were generated from an SQL query.
// Contains a list of operators trees accessible by their roots.
// In SQL each operator tree corresponds to a single statement.
// Since a query can contain multiple statement, this query plan can hold the roots of each statement.
// When caching a query (through prepared statements or automatically) its SQLQueryPlan object is cached.
class SQLQueryPlan {
 public:
  SQLQueryPlan();

  // Add a new operator tree to the query plan by adding the root operator.
  void add_tree_by_root(std::shared_ptr<AbstractOperator> op);

  // Append all operator trees from the other plan.
  void append_plan(const SQLQueryPlan& other_plan);

  // Wrap all operator trees in tasks and return them.
  std::vector<std::shared_ptr<OperatorTask>> create_tasks() const;

  // Returns the root nodes of all operator trees in the plan.
  const std::vector<std::shared_ptr<AbstractOperator>>& tree_roots() const;

  // Recreates the query plan with a new and equivalent set of operator trees.
  // The given list of arguments is passed to the recreate method of all operators to replace ValuePlaceholders.
  SQLQueryPlan recreate(const std::vector<AllParameterVariant>& arguments = {}) const;

  // Calls set_transaction_context_recursively on all roots.
  void set_transaction_context(std::shared_ptr<TransactionContext> context);

  // Set the number of parameters that this query plan contains.
  void set_num_parameters(uint16_t num_parameters);

  // Get the number of parameters that this query plan contains.
  uint16_t num_parameters() const;

 protected:
  // Root nodes of all operator trees that this plan contains.
  std::vector<std::shared_ptr<AbstractOperator>> _roots;

  // Number of PlaceholderValues within the plan's operators.
  uint16_t _num_parameters;
};

}  // namespace opossum
