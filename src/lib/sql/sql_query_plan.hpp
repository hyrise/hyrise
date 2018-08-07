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
  explicit SQLQueryPlan(CleanupTemporaries cleanup_temporaries);

  // Add a new operator tree to the query plan by adding the root operator.
  void add_tree_by_root(const std::shared_ptr<AbstractOperator>& op);

  // Append all operator trees from the other plan.
  void append_plan(const SQLQueryPlan& other_plan);

  // Wrap all operator trees in tasks and return them.
  std::vector<std::shared_ptr<OperatorTask>> create_tasks() const;

  // Returns the root nodes of all operator trees in the plan.
  const std::vector<std::shared_ptr<AbstractOperator>>& tree_roots() const;

  // Recreates the query plan with a new and equivalent set of operator trees.
  SQLQueryPlan deep_copy() const;

  // Calls set_transaction_context_recursively on all roots.
  void set_transaction_context(const std::shared_ptr<TransactionContext>& context);

  // Set the parameter ids of the value placeholders
  void set_parameter_ids(const std::unordered_map<ValuePlaceholderID, ParameterID>& parameter_ids);
  const std::unordered_map<ValuePlaceholderID, ParameterID>& parameter_ids() const;

 protected:
  // Should we delete temporary result tables once they are not needed anymore?
  CleanupTemporaries _cleanup_temporaries;

  // Root nodes of all operator trees that this plan contains.
  std::vector<std::shared_ptr<AbstractOperator>> _roots;
  std::unordered_map<ValuePlaceholderID, ParameterID> _parameter_ids;
};

}  // namespace opossum
